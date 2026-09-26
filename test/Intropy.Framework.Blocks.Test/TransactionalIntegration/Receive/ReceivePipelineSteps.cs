using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.TransactionalIntegration.Receive;

public sealed record ReceiveContext() : Context(new Dictionary<string, string>())
{
    public static ReceiveContext Create() => new();
}

/// <summary>
/// Test implementation that successfully publishes to queue.
/// </summary>
public sealed class SuccessfulEnqueuer(FrameworkOptions fwOptions) : EnqueueStep<ReceiveContext>(fwOptions)
{
    public List<SourceItem> Published { get; } = [];

    public override Task<(TechnicalStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, ReceiveContext context, CancellationToken ct)
    {
        Published.Add(input);
        return Task.FromResult<(TechnicalStepResult<SourceItem>, ReceiveContext)>(
            (new TechnicalStepResult<SourceItem>.Success(input), context));
    }
}

/// <summary>
/// Test implementation whose queue is unavailable (simulates a dead broker).
/// </summary>
public sealed class FailingEnqueuer(FrameworkOptions fwOptions) : EnqueueStep<ReceiveContext>(fwOptions)
{
    public override Task<(TechnicalStepResult<SourceItem> Result, ReceiveContext Context)> ExecuteAsync(
        SourceItem input, ReadOnlyMemory<byte> cloudEvent, ReceiveContext context, CancellationToken ct)
    {
        var failure = new TechnicalFailure(Description: "Queue unavailable", Exception: new HttpRequestException("broker is dead"));
        return Task.FromResult<(TechnicalStepResult<SourceItem>, ReceiveContext)>(
            (new TechnicalStepResult<SourceItem>.Failure(failure), context));
    }
}
