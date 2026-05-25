using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Intropy.Framework.Core.Pipeline.Core;

namespace Intropy.Framework.Core.Test.Pipeline.Core;

public class PipelineOptionalStepTests
{
    private static readonly BusinessIncidentData Incident = new() { Description = "test failure", Context = [] };
    private static readonly TechnicalFailure Failure = new("test failure");

    private static Task<(StepResult<string> Result, string Context, CancellationToken CancellationToken)> InputWith(
        StepResult<string> result, CancellationToken ct = default) =>
        Task.FromResult<(StepResult<string>, string, CancellationToken)>((result, "ctx", ct));

    #region AddOptionalStep (Step)

    [Fact]
    public async Task AddOptionalStep_Step_WhenNull_PassesThroughSuccess()
    {
        Step<string, string, string>? step = null;

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello", ((StepResult<string>.Success)result).Value);
        Assert.Equal("ctx", context);
    }

    [Fact]
    public async Task AddOptionalStep_Step_WhenNull_PassesThroughBusinessFailure()
    {
        Step<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.BusinessFailure(Incident))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.BusinessFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_Step_WhenNull_PassesThroughTechnicalFailure()
    {
        Step<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.TechnicalFailure(Failure))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_Step_WhenNull_PassesThroughCancelled()
    {
        Step<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Cancelled())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Cancelled>(result);
    }

    [Fact]
    public async Task AddOptionalStep_Step_WhenNull_PassesThroughAborted()
    {
        Step<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Aborted())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task AddOptionalStep_Step_WhenProvided_ExecutesStep()
    {
        Step<string, string, string> step = new AppendBaseStep("_base");

        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello_base", ((StepResult<string>.Success)result).Value);
    }

    #endregion

    #region AddOptionalStep (BusinessStep)

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenNull_PassesThroughSuccess()
    {
        BusinessStep<string, string, string>? step = null;

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello", ((StepResult<string>.Success)result).Value);
        Assert.Equal("ctx", context);
    }

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenNull_PassesThroughBusinessFailure()
    {
        BusinessStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.BusinessFailure(Incident))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.BusinessFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenNull_PassesThroughTechnicalFailure()
    {
        BusinessStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.TechnicalFailure(Failure))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenNull_PassesThroughCancelled()
    {
        BusinessStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Cancelled())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Cancelled>(result);
    }

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenNull_PassesThroughAborted()
    {
        BusinessStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Aborted())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task AddOptionalStep_BusinessStep_WhenProvided_ExecutesStep()
    {
        BusinessStep<string, string, string> step = new AppendBusinessStep("_suffix");

        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello_suffix", ((StepResult<string>.Success)result).Value);
    }

    #endregion

    #region AddOptionalStep (TechnicalStep)

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenNull_PassesThroughSuccess()
    {
        TechnicalStep<string, string, string>? step = null;

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello", ((StepResult<string>.Success)result).Value);
        Assert.Equal("ctx", context);
    }

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenNull_PassesThroughBusinessFailure()
    {
        TechnicalStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.BusinessFailure(Incident))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.BusinessFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenNull_PassesThroughTechnicalFailure()
    {
        TechnicalStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.TechnicalFailure(Failure))
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenNull_PassesThroughCancelled()
    {
        TechnicalStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Cancelled())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Cancelled>(result);
    }

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenNull_PassesThroughAborted()
    {
        TechnicalStep<string, string, string>? step = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Aborted())
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task AddOptionalStep_TechnicalStep_WhenProvided_ExecutesStep()
    {
        TechnicalStep<string, string, string> step = new AppendTechnicalStep("_tech");

        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalStep(step);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello_tech", ((StepResult<string>.Success)result).Value);
    }

    #endregion

    #region AddOptionalFinalizer

    [Fact]
    public async Task AddOptionalFinalizer_WhenNull_PassesThroughSuccess()
    {
        Finalizer<string, string>? finalizer = null;

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("hello", ((StepResult<string>.Success)result).Value);
        Assert.Equal("ctx", context);
    }

    [Fact]
    public async Task AddOptionalFinalizer_WhenNull_PassesThroughBusinessFailure()
    {
        Finalizer<string, string>? finalizer = null;

        var (result, _, _) = await InputWith(new StepResult<string>.BusinessFailure(Incident))
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.BusinessFailure>(result);
    }

    [Fact]
    public async Task AddOptionalFinalizer_WhenNull_PassesThroughTechnicalFailure()
    {
        Finalizer<string, string>? finalizer = null;

        var (result, _, _) = await InputWith(new StepResult<string>.TechnicalFailure(Failure))
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task AddOptionalFinalizer_WhenNull_PassesThroughCancelled()
    {
        Finalizer<string, string>? finalizer = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Cancelled())
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.Cancelled>(result);
    }

    [Fact]
    public async Task AddOptionalFinalizer_WhenNull_PassesThroughAborted()
    {
        Finalizer<string, string>? finalizer = null;

        var (result, _, _) = await InputWith(new StepResult<string>.Aborted())
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.Aborted>(result);
    }

    [Fact]
    public async Task AddOptionalFinalizer_WhenProvided_ExecutesFinalizer()
    {
        Finalizer<string, string> finalizer = new ContextAppendFinalizer();

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("hello", "ctx")
            .AddOptionalFinalizer(finalizer);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("ctx_finalized", context);
    }

    #endregion

    #region Mixed

    [Fact]
    public async Task Pipeline_WithMixedOptionalAndRequiredSteps_ExecutesCorrectly()
    {
        BusinessStep<string, string, string>? nullStep = null;
        BusinessStep<string, string, string> presentStep = new AppendBusinessStep("_present");
        Finalizer<string, string>? nullFinalizer = null;

        var (result, _, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("input", "ctx")
            .AddStep(new AppendBusinessStep("_required"))
            .AddOptionalStep(nullStep)
            .AddOptionalStep(presentStep)
            .AddOptionalFinalizer(nullFinalizer);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("input_required_present", ((StepResult<string>.Success)result).Value);
    }

    [Fact]
    public async Task Pipeline_WithAllOptionalStepsNull_OnlyRunsRequiredSteps()
    {
        BusinessStep<string, string, string>? nullBusiness = null;
        TechnicalStep<string, string, string>? nullTechnical = null;
        Step<string, string, string>? nullBase = null;
        Finalizer<string, string>? nullFinalizer = null;

        var (result, context, _) = await Intropy.Framework.Core.Pipeline.Core.Pipeline
            .Start("input", "ctx")
            .AddOptionalStep(nullBusiness)
            .AddOptionalStep(nullTechnical)
            .AddOptionalStep(nullBase)
            .AddOptionalFinalizer(nullFinalizer);

        Assert.IsType<StepResult<string>.Success>(result);
        Assert.Equal("input", ((StepResult<string>.Success)result).Value);
        Assert.Equal("ctx", context);
    }

    #endregion
}

internal sealed class AppendBusinessStep(string suffix) : BusinessStep<string, string, string>
{
    public override string StepName => "AppendBusiness";

    public override Task<(BusinessStepResult<string> Result, string Context)> ExecuteAsync(
        string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(BusinessStepResult<string>, string)>(
            (new BusinessStepResult<string>.Success(input + suffix), context));
    }
}

internal sealed class AppendTechnicalStep(string suffix) : TechnicalStep<string, string, string>
{
    public override string StepName => "AppendTechnical";

    public override Task<(TechnicalStepResult<string> Result, string Context)> ExecuteAsync(
        string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(TechnicalStepResult<string>, string)>(
            (new TechnicalStepResult<string>.Success(input + suffix), context));
    }
}

internal sealed class AppendBaseStep(string suffix) : Step<string, string, string>
{
    public override string StepName => "AppendBase";

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(
        string input, string context, CancellationToken ct)
    {
        return Task.FromResult<(StepResult<string>, string)>(
            (new StepResult<string>.Success(input + suffix), context));
    }
}

internal sealed class ContextAppendFinalizer : Finalizer<string, string>
{
    public override string FinalizerName => "ContextAppend";
    public override FinalizerTrigger Triggers => FinalizerTrigger.OnSuccess;

    public override Task<(StepResult<string> Result, string Context)> ExecuteAsync(
        StepResult<string> result, string context, CancellationToken ct)
    {
        return Task.FromResult((result, context + "_finalized"));
    }
}
