using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;
using Intropy.Framework.Core.Pipeline.Core;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

/// <summary>A step that appends its name to the input, so the test can observe how many times (and
/// on what value) the chain called it.</summary>
file sealed class AppendStep(string suffix) : Step<string, string, Context>
{
    public override string StepName => $"Append.{suffix}";

    public int CallCount { get; private set; }

    public override Task<(StepResult<string> Result, Context Context)> ExecuteAsync(
        string input, Context context, CancellationToken ct)
    {
        CallCount++;
        return Task.FromResult<(StepResult<string>, Context)>((new StepResult<string>.Success(input + suffix), context));
    }
}

file sealed class RecordingFinalizer : Finalizer<string, Context>
{
    public override string FinalizerName => "Recording";
    public override FinalizerTrigger Triggers =>
        FinalizerTrigger.OnSuccess | FinalizerTrigger.OnBusinessFailure | FinalizerTrigger.OnTechnicalFailure |
        FinalizerTrigger.OnCancelled | FinalizerTrigger.OnAborted;

    public StepResult<string>? Observed { get; private set; }

    public override Task<(StepResult<string> Result, Context Context)> ExecuteAsync(
        StepResult<string> result, Context context, CancellationToken ct)
    {
        Observed = result;
        return Task.FromResult((result, context));
    }
}

public class BulkPipelineTests
{
    private static Context NewContext() => new([]);

    [Fact]
    public async Task Resume_WithSuccess_ContinuesTheChainLikeStart()
    {
        var append = new AppendStep("-B");

        var (result, _, _) = await BulkPipeline.Resume(new StepResult<string>.Success("A"), NewContext())
            .AddStep(append);

        Assert.Equal(1, append.CallCount);
        Assert.Equal(new StepResult<string>.Success("A-B"), result);
    }

    [Fact]
    public async Task Resume_WithBusinessFailure_ShortCircuitsSubsequentSteps()
    {
        var append = new AppendStep("-B");
        var incident = new BusinessIncidentData { Description = "pre-send failure" };

        var (result, _, _) = await BulkPipeline
            .Resume(new StepResult<string>.BusinessFailure(incident, Retriable: true), NewContext())
            .AddStep(append);

        Assert.Equal(0, append.CallCount);
        var failure = Assert.IsType<StepResult<string>.BusinessFailure>(result);
        Assert.Same(incident, failure.Value);
        Assert.True(failure.Retriable);
    }

    [Fact]
    public async Task Resume_WithTechnicalFailure_ShortCircuitsSubsequentSteps()
    {
        var append = new AppendStep("-B");
        var failure = new TechnicalFailure("boom", null);

        var (result, _, _) = await BulkPipeline
            .Resume(new StepResult<string>.TechnicalFailure(failure), NewContext())
            .AddStep(append);

        Assert.Equal(0, append.CallCount);
        Assert.IsType<StepResult<string>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Resume_WithCancelled_ShortCircuitsSubsequentSteps()
    {
        var append = new AppendStep("-B");

        var (result, _, _) = await BulkPipeline.Resume(new StepResult<string>.Cancelled(), NewContext())
            .AddStep(append);

        Assert.Equal(0, append.CallCount);
        Assert.IsType<StepResult<string>.Cancelled>(result);
    }

    [Fact]
    public async Task Resume_FollowedByFinalizer_RunsTheFinalizerExactlyOnce_WhateverTheIncomingResult()
    {
        var finalizer = new RecordingFinalizer();
        var incident = new BusinessIncidentData { Description = "send failure" };

        var (result, _, _) = await BulkPipeline
            .Resume(new StepResult<string>.BusinessFailure(incident, Retriable: false), NewContext())
            .AddFinalizer(finalizer);

        Assert.NotNull(finalizer.Observed);
        Assert.IsType<StepResult<string>.BusinessFailure>(finalizer.Observed);
        Assert.Equal(result, finalizer.Observed);
    }
}
