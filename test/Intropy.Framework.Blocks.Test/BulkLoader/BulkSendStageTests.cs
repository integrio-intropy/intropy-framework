using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

file sealed class RecordingBatchSender : BatchSendStep<string, Context>
{
    private readonly Func<IReadOnlyList<(string Value, Context Context)>, IReadOnlyList<StepResult<string>>> _respond;

    public RecordingBatchSender(Func<IReadOnlyList<(string Value, Context Context)>, IReadOnlyList<StepResult<string>>> respond) =>
        _respond = respond;

    public List<IReadOnlyList<(string Value, Context Context)>> Calls { get; } = [];

    public override Task<IReadOnlyList<StepResult<string>>> ExecuteBatchAsync(
        IReadOnlyList<(string Value, Context Context)> entries, CancellationToken ct)
    {
        Calls.Add(entries);
        return Task.FromResult(_respond(entries));
    }
}

file sealed class ThrowingBatchSender(Exception exception) : BatchSendStep<string, Context>
{
    public override Task<IReadOnlyList<StepResult<string>>> ExecuteBatchAsync(
        IReadOnlyList<(string Value, Context Context)> entries, CancellationToken ct) => throw exception;
}

file sealed class MiscountingBatchSender : BatchSendStep<string, Context>
{
    public override Task<IReadOnlyList<StepResult<string>>> ExecuteBatchAsync(
        IReadOnlyList<(string Value, Context Context)> entries, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<StepResult<string>>>([new StepResult<string>.Success("only one")]);
}

public class BulkSendStageTests
{
    private static Context NewContext() => new([]);

    private static (StepResult<string> Result, Context Context, CancellationToken CancellationToken)[] Batch(
        params StepResult<string>[] results) =>
        results.Select(r => (r, NewContext(), CancellationToken.None)).ToArray();

    [Fact]
    public async Task ApplyAsync_AllSuccess_CallsTheBatchStepOnceWithEveryEntry()
    {
        var sender = new RecordingBatchSender(entries =>
            entries.Select(e => (StepResult<string>)new StepResult<string>.Success(e.Value + "-sent")).ToList());

        var batch = Batch(new StepResult<string>.Success("a"), new StepResult<string>.Success("b"));

        var output = await BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None);

        Assert.Single(sender.Calls);
        Assert.Equal(2, sender.Calls[0].Count);
        Assert.Equal(["a-sent", "b-sent"],
            output.Select(o => ((StepResult<string>.Success)o.Result).Value));
    }

    [Fact]
    public async Task ApplyAsync_MixedBatch_SkipsNonSuccessEntriesAndPassesThemThroughUnchanged()
    {
        var sender = new RecordingBatchSender(entries =>
            entries.Select(e => (StepResult<string>)new StepResult<string>.Success(e.Value + "-sent")).ToList());

        var preFailure = new StepResult<string>.BusinessFailure(
            new BusinessIncidentData { Description = "pre-send failure" }, Retriable: false);
        var batch = Batch(new StepResult<string>.Success("a"), preFailure, new StepResult<string>.Cancelled());

        var output = await BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None);

        // Only the Success entry reaches the batch step.
        Assert.Single(sender.Calls);
        Assert.Single(sender.Calls[0]);
        Assert.Equal("a", sender.Calls[0][0].Value);

        Assert.Equal("a-sent", ((StepResult<string>.Success)output[0].Result).Value);
        Assert.Same(preFailure, output[1].Result);
        Assert.IsType<StepResult<string>.Cancelled>(output[2].Result);
    }

    [Fact]
    public async Task ApplyAsync_PartialRejection_GivesEachEntryItsOwnVerdict()
    {
        var sender = new RecordingBatchSender(entries => entries.Select(e =>
            e.Value == "bad"
                ? (StepResult<string>)new StepResult<string>.BusinessFailure(
                    new BusinessIncidentData { Description = "rejected by downstream" })
                : new StepResult<string>.Success(e.Value + "-sent")).ToList());

        var batch = Batch(new StepResult<string>.Success("good"), new StepResult<string>.Success("bad"));

        var output = await BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None);

        Assert.Equal("good-sent", ((StepResult<string>.Success)output[0].Result).Value);
        var rejected = Assert.IsType<StepResult<string>.BusinessFailure>(output[1].Result);
        Assert.Equal("rejected by downstream", rejected.Value.Description);
    }

    [Fact]
    public async Task ApplyAsync_WholeBatchException_GivesEveryEntryTheSameBusinessFailure()
    {
        var sender = new ThrowingBatchSender(new TimeoutException("downstream unreachable"));
        var batch = Batch(new StepResult<string>.Success("a"), new StepResult<string>.Success("b"));

        var output = await BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None);

        var first = Assert.IsType<StepResult<string>.BusinessFailure>(output[0].Result);
        var second = Assert.IsType<StepResult<string>.BusinessFailure>(output[1].Result);
        Assert.Contains("downstream unreachable", first.Value.Description);
        Assert.Equal(first.Value.Description, second.Value.Description);
    }

    [Fact]
    public async Task ApplyAsync_NoSuccessEntries_NeverCallsTheBatchStep()
    {
        var sender = new RecordingBatchSender(entries => []);
        var batch = Batch(new StepResult<string>.Cancelled(), new StepResult<string>.Cancelled());

        var output = await BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None);

        Assert.Empty(sender.Calls);
        Assert.All(output, o => Assert.IsType<StepResult<string>.Cancelled>(o.Result));
    }

    [Fact]
    public async Task ApplyAsync_ImplementationReturnsWrongCount_Throws()
    {
        var sender = new MiscountingBatchSender();
        var batch = Batch(new StepResult<string>.Success("a"), new StepResult<string>.Success("b"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            BulkSendStage.ApplyAsync(batch, sender, CancellationToken.None));
    }
}
