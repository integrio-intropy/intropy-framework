using System.Diagnostics;
using Intropy.Framework.Core.Common;
using Intropy.Framework.Core.Pipeline.Abstractions.Failures;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Core.Pipeline.Core;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace Intropy.Framework.Core.Test.Pipeline.Core;

public class PipelineTracingTests
{
    private const string FailureDescription = "error";
    private const string PipelineName = "TestPipeline";

    private readonly ILogger _logger = Substitute.For<ILogger>();

    [Fact]
    public async Task ExecuteWithTracing_WhenFuncReturnsFailure_SetsActivityStatusToError()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult =
            new StepResult<string>.TechnicalFailure(new TechnicalFailure(FailureDescription));
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            var (result, _) = await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger);

            // Assert
            Assert.IsType<StepResult<string>.TechnicalFailure>(result);
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Equal(ActivityStatusCode.Error, capturedActivity.Status);
            Assert.Equal(FailureDescription, capturedActivity.StatusDescription);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_WhenFuncReturnsSuccess_SetsActivityStatusToOk()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Success(string.Empty);
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            var (result, _) = await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger);

            // Assert
            Assert.IsType<StepResult<string>.Success>(result);
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Equal(ActivityStatusCode.Ok, capturedActivity.Status);
            Assert.Equal($"Pipeline.{PipelineName}", capturedActivity.DisplayName);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_ShouldSetLinkToParent_WhenDetachIsTrue_And_ParentSpanExists()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Success(string.Empty);
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            using var activitySource = new ActivitySource(ActivitySourceProvider.ActivitySourceName);
            using var activity = activitySource.StartActivity();
            await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger, detachTrace: true);

            // Assert
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Single(capturedActivity.Links, x => x.Context.SpanId == activity!.SpanId);
            Assert.Null(capturedActivity.Parent);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_ShouldNotSetLinkToParent_WhenDetachIsTrue_And_NoParentSpanExists()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Success(string.Empty);
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger, detachTrace: true);

            // Assert
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Null(capturedActivity.Parent);
            Assert.Empty(capturedActivity.Links);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_ShouldNotDetach_WhenDetachIsFalse()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Success(string.Empty);
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            using var activitySource = new ActivitySource(ActivitySourceProvider.ActivitySourceName);
            using var activity = activitySource.StartActivity();
            await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger, detachTrace: false);

            // Assert
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Empty(capturedActivity.Links);
            Assert.NotNull(capturedActivity.Parent);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_SetsAdditionalTags()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Success(string.Empty);
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        var expectedTag = new KeyValuePair<string, string>("test", "test123");

        try
        {
            // Act
            var (result, _) = await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger,
                activity => { activity?.SetTag(expectedTag.Key, expectedTag.Value); });

            // Assert
            Assert.IsType<StepResult<string>.Success>(result);
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Equal(ActivityStatusCode.Ok, capturedActivity.Status);
            Assert.Equal($"Pipeline.{PipelineName}", capturedActivity.DisplayName);

            Assert.Contains(capturedActivity.Tags, a => a.Key == expectedTag.Key && a.Value == expectedTag.Value);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ExecuteWithTracing_WhenFuncReturnsAborted_SetsActivityStatusToOk()
    {
        // Arrange
        var activities = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceProvider.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);

        StepResult<string> expectedResult = new StepResult<string>.Aborted();
        var expectedContext = string.Empty;
        var pipeline = () => Task.FromResult((Result: expectedResult, Context: expectedContext, CancellationToken: CancellationToken.None));

        try
        {
            // Act
            var (result, _) = await PipelineTracing.ExecuteWithTracing(pipeline, PipelineName, _logger);

            // Assert
            Assert.IsType<StepResult<string>.Aborted>(result);
            var capturedActivity = activities.Single(a => a.DisplayName == $"Pipeline.{PipelineName}");
            Assert.Equal(ActivityStatusCode.Ok, capturedActivity.Status);
        }
        finally
        {
            listener.Dispose();
        }
    }
}
