using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Intropy.Framework.Core.Pipeline.Core;

namespace Intropy.Framework.Core.Test.Pipeline.Core;

public class PipelineTests
{
    [Fact]
    public async Task BusinessStep_WithUncaughtException_ShouldBecomeBusinessIncident()
    {
        // Arrange
        // ReSharper disable once ConvertToLocalFunction
        var pipeline = () => Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "")
            .AddStep(new ThrowingBusinessStep());

        // Act
        var (result, _, _) = await pipeline();

        // Assert
        Assert.IsType<StepResult<string>.BusinessFailure>(result);
    }

    [Fact]
    public async Task AddSteps_WithMultipleSteps_ExecutesAllInOrder()
    {
        // Arrange
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Success("a"), "", CancellationToken.None));
        var steps = new[] { new AppendStep("b"), new AppendStep("c"), new AppendStep("d") };

        // Act
        var (result, _, _) = await input.AddSteps(steps);

        // Assert
        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("abcd", ((StepResult<string>.Success)result).Value);
    }

    [Fact]
    public async Task AddSteps_WithEmptyCollection_ReturnsInputUnchanged()
    {
        // Arrange
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Success("input"), "1", CancellationToken.None));
        var steps = Enumerable.Empty<BusinessStep<string, string, string>>();

        // Act
        var (result, context, _) = await input.AddSteps(steps);

        // Assert
        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("input", ((StepResult<string>.Success)result).Value);
        Assert.Equal("1", context);
    }

    [Fact]
    public async Task TechnicalStep_WithUncaughtException_ShouldBecomeTechnicalIncident()
    {
        // Arrange
        var pipeline = () => Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "")
            .AddStep(new ThrowingTechnicalStep());

        // Act
        var (result, _, _) = await pipeline();

        // Assert
        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Step_WithUncaughtException_ShouldBecomeTechnicalIncident()
    {
        // Arrange
        var pipeline = () => Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "")
            .AddStep(new ThrowingStep());

        // Act
        var (result, _, _) = await pipeline();

        // Assert
        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Finalizer_WithUncaughtException_ShouldBecomeTechnicalIncident()
    {
        // Arrange
        var pipeline = () => Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "")
            .AddFinalizer(new ThrowingFinalizer());

        // Act
        var (result, _, _) = await pipeline();

        // Assert
        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Step_WithCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "", cts.Token)
            .AddStep(new SuccessStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task BusinessStep_WithCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "", cts.Token)
            .AddStep(new AppendStep("x"));

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task TechnicalStep_WithCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "", cts.Token)
            .AddStep(new SuccessTechnicalStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task Finalizer_WithCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("", "", cts.Token)
            .AddFinalizer(new SuccessFinalizer(FinalizerTrigger.OnSuccess));

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task Step_WithOperationCanceledExceptionAndCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Success(""), "", cts.Token));

        // Act
        var (result, _, _) = await input.AddStep(new CancellingStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task BusinessStep_WithOperationCanceledExceptionAndCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Success(""), "", cts.Token));

        // Act
        var (result, _, _) = await input.AddStep(new CancellingBusinessStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task TechnicalStep_WithOperationCanceledExceptionAndCancelledToken_ShouldReturnAborted()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Success(""), "", cts.Token));

        // Act
        var (result, _, _) = await input.AddStep(new CancellingTechnicalStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task AddStep_WithAbortedInput_ShouldPropagateAborted()
    {
        // Arrange
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Aborted(), "", CancellationToken.None));

        // Act
        var (result, _, _) = await input.AddStep(new SuccessStep());

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task Finalizer_WithOnAbortedTrigger_ShouldRunOnAbortedResult()
    {
        // Arrange
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Aborted(), "", CancellationToken.None));
        var finalizer = new SuccessFinalizer(FinalizerTrigger.OnAborted);

        // Act
        var (result, _, _) = await input.AddFinalizer(finalizer);

        // Assert
        Assert.IsType<StepResult<string>.Success>(result);
    }

    [Fact]
    public async Task Finalizer_WithoutOnAbortedTrigger_ShouldNotRunOnAbortedResult()
    {
        // Arrange
        var input = Task.FromResult<(StepResult<string> Result, string Context, CancellationToken CancellationToken)>(
            (new StepResult<string>.Aborted(), "", CancellationToken.None));
        var finalizer = new SuccessFinalizer(FinalizerTrigger.OnSuccess);

        // Act
        var (result, _, _) = await input.AddFinalizer(finalizer);

        // Assert
        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task Pipeline_WithDefaultToken_ShouldExecuteNormally()
    {
        // Arrange & Act
        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline.Start("value", "ctx")
            .AddStep(new SuccessStep());

        // Assert
        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("value", ((StepResult<string>.Success)result).Value);
    }
}

internal sealed class ThrowingBusinessStep : BusinessStep<string, string, string>
{
    public override string StepName => "ThrowingBusinessStep";

    public override Task<(BusinessStepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        throw new InvalidOperationException("ThrowingBusinessStep");
    }
}

internal sealed class ThrowingTechnicalStep : TechnicalStep<string, string, string>
{
    public override string StepName => "ThrowingTechnicalStep";

    public override Task<(TechnicalStepResult<string> Result, string Context)> ExecuteAsync(string input,
        string context, CancellationToken ct)
    {
        throw new InvalidOperationException("ThrowingTechnicalStep");
    }
}

internal sealed class ThrowingStep : Step<string, string, string>
{
    public override string StepName => "ThrowingStep";

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        throw new InvalidOperationException("ThrowingStep");
    }
}

internal sealed class ThrowingFinalizer : Finalizer<string, string>
{
    public override string FinalizerName => "ThrowingFinalizer";

    public override FinalizerTrigger Triggers => FinalizerTrigger.OnSuccess |
                                                 FinalizerTrigger.OnCancelled |
                                                 FinalizerTrigger.OnBusinessFailure |
                                                 FinalizerTrigger.OnTechnicalFailure;

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(StepResult<string> result,
        string context, CancellationToken ct)
    {
        throw new InvalidOperationException("ThrowingFinalizer");
    }
}

internal sealed class AppendStep(string suffix) : BusinessStep<string, string, string>
{
    public override string StepName => "Append";

    public override Task<(BusinessStepResult<string> Result, string Context)> ExecuteAsync(
        string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(BusinessStepResult<string>, string)>(
            (new BusinessStepResult<string>.Success(input + suffix), context));
    }
}

internal sealed class SuccessStep : Step<string, string, string>
{
    public override string StepName => "SuccessStep";

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(StepResult<string>, string)>(
            (new StepResult<string>.Success(input), context));
    }
}

internal sealed class SuccessTechnicalStep : TechnicalStep<string, string, string>
{
    public override string StepName => "SuccessTechnicalStep";

    public override Task<(TechnicalStepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(TechnicalStepResult<string>, string)>(
            (new TechnicalStepResult<string>.Success(input), context));
    }
}

internal sealed class CancellingStep : Step<string, string, string>
{
    public override string StepName => "CancellingStep";

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        throw new OperationCanceledException();
    }
}

internal sealed class CancellingBusinessStep : BusinessStep<string, string, string>
{
    public override string StepName => "CancellingBusinessStep";

    public override Task<(BusinessStepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        throw new OperationCanceledException();
    }
}

internal sealed class CancellingTechnicalStep : TechnicalStep<string, string, string>
{
    public override string StepName => "CancellingTechnicalStep";

    public override Task<(TechnicalStepResult<string> Result, string Context)> ExecuteAsync(string input, string context, CancellationToken ct)
    {
        throw new OperationCanceledException();
    }
}

internal sealed class SuccessFinalizer(FinalizerTrigger triggers) : Finalizer<string, string>
{
    public override string FinalizerName => "SuccessFinalizer";

    public override FinalizerTrigger Triggers => triggers;

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(StepResult<string> result, string context, CancellationToken ct)
    {
        return Task.FromResult<(StepResult<string>, string)>(
            (new StepResult<string>.Success("finalized"), context));
    }
}
