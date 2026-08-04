using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Blocks.Test.Extractor.Steps;

public class CloudEventSerializeStepTests
{
    private static CloudEventSerializeStep<CustomerOut, Context> CreateStep(
        Func<CustomerOut, string>? subject = null,
        Func<CustomerOut, DateTimeOffset>? time = null,
        string dataContentType = "application/json")
    {
        return new CloudEventSerializeStep<CustomerOut, Context>(
            subject ?? (c => c.CustomerId.ToString()),
            time ?? (_ => DateTimeOffset.UtcNow),
            dataContentType);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullSubject_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new CloudEventSerializeStep<CustomerOut, Context>(
                null!,
                _ => DateTimeOffset.UtcNow));

        Assert.Equal("subject", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullTime_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new CloudEventSerializeStep<CustomerOut, Context>(
                c => c.CustomerId.ToString(),
                null!));

        Assert.Equal("time", exception.ParamName);
    }

    #endregion

    #region ExecuteAsync - Happy Path Tests

    [Fact]
    public async Task ExecuteAsync_WithValidExtractors_ReturnsSuccessWithEquivalentEnvelope()
    {
        // Arrange
        var input = new CustomerOut { CustomerId = 1337 };
        var time = new DateTimeOffset(2025, 4, 24, 10, 30, 0, TimeSpan.Zero);
        var step = CreateStep(c => c.CustomerId.ToString(), _ => time);
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, resultContext) = await step.ExecuteAsync(input, context, CancellationToken.None);

        // Assert
        var successResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        var cloudEvent = successResult.Value;
        Assert.Equal("1337", cloudEvent.Subject);
        Assert.Equal(time, cloudEvent.Time);
        Assert.Equal("application/json", cloudEvent.DataContentType);
        Assert.Same(input, cloudEvent.Data);
        Assert.True(Guid.TryParse(cloudEvent.Id, out _));
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_CalledTwice_ProducesDistinctIds()
    {
        // Arrange
        var step = CreateStep();
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (first, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);
        var (second, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var firstEvent = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(first).Value;
        var secondEvent = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(second).Value;
        Assert.NotEqual(firstEvent.Id, secondEvent.Id);
    }

    [Fact]
    public async Task ExecuteAsync_WithCustomDataContentType_IsHonored()
    {
        // Arrange
        var step = CreateStep(dataContentType: "application/avro");
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var successResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Equal("application/avro", successResult.Value.DataContentType);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotSetSourceAndType()
    {
        // Arrange
        var step = CreateStep();
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert - Source and Type are owned by the send step (e.g. DaprTopicPublisher)
        var successResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Success>(result);
        Assert.Null(successResult.Value.Source);
        Assert.Null(successResult.Value.Type);
    }

    #endregion

    #region ExecuteAsync - Validation Failure Tests

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ExecuteAsync_WithInvalidSubject_ReturnsTechnicalFailureNamingSubject(string? subjectValue)
    {
        // Arrange
        var step = CreateStep(subject: _ => subjectValue!);
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, resultContext) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var failureResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Failure>(result);
        Assert.Contains("Subject", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithDefaultTime_ReturnsTechnicalFailureNamingTime()
    {
        // Arrange
        var step = CreateStep(time: _ => default);
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, resultContext) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var failureResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Failure>(result);
        Assert.Contains("Time", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithValidSubjectAndDefaultTime_FailureNamesTimeOnly()
    {
        // Arrange
        var step = CreateStep(subject: _ => "customer-1", time: _ => default);
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var failureResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Failure>(result);
        Assert.Contains("Time", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.DoesNotContain("Subject", failureResult.Value.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidSubjectAndDefaultTime_FailureNamesSubjectFirst()
    {
        // Arrange
        var step = CreateStep(subject: _ => null!, time: _ => default);
        var context = new Context(new Dictionary<string, string>());

        // Act
        var (result, _) = await step.ExecuteAsync(new CustomerOut { CustomerId = 1 }, context, CancellationToken.None);

        // Assert
        var failureResult = Assert.IsType<TechnicalStepResult<CloudEvent>.Failure>(result);
        Assert.Contains("Subject", failureResult.Value.Description, StringComparison.Ordinal);
    }

    #endregion
}
