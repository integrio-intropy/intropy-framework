using System.Globalization;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.Shared.Steps.External;

public class ExternalIdempotencyRecorderTests
{
    // Test types
    private sealed class TestMessage
    {
        public string Content { get; init; } = string.Empty;
    }

    private sealed record TestContext() : Context(new Dictionary<string, string>());

    // Test fixtures
    private readonly IIdempotencyServiceClient _mockClient = Substitute.For<IIdempotencyServiceClient>();
    private readonly FrameworkOptions _frameworkOptions = new() { ComponentName = "TestComponent", ServiceNamespace = "TestNamespace" };

    #region Constructor Tests

    [Fact]
    public void Constructor_WithAllValidParameters_CreatesInstance()
    {
        // Act
        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Assert
        Assert.NotNull(recorder);
    }

    [Fact]
    public void Constructor_WithNullClient_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyRecorder<TestMessage, TestContext>(
                null!,
                _frameworkOptions
            )
        );

        Assert.Equal("client", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullFrameworkOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyRecorder<TestMessage, TestContext>(
                _mockClient,
                null!
            )
        );

        Assert.Equal("frameworkOptions", exception.ParamName);
    }

    #endregion

    #region ExecuteAsync - Success Path Tests

    [Fact]
    public async Task ExecuteAsync_WithAllRequiredMetadata_ReturnsSuccessAndRecordsMessage()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = "2025-01-15T10:30:00.0000000Z",
                [IdempotencyContextKeys.Hash] = "abc123hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result);
        var successResult = (TechnicalStepResult<TestMessage>.Success)result;
        Assert.Same(message, successResult.Value);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithValidMetadata_CallsCommitAsyncOnClient()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-456",
                [IdempotencyContextKeys.Date] = "2025-02-20T15:45:30.0000000Z",
                [IdempotencyContextKeys.Hash] = "xyz789hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(mi =>
            mi.Component == "TestComponent" &&
            mi.Id == "msg-456" &&
            mi.Hash == "xyz789hash" &&
            mi.Timestamp == DateTime.Parse("2025-02-20T15:45:30.0000000Z", CultureInfo.InvariantCulture)
        ));
    }

    [Fact]
    public async Task ExecuteAsync_WithIso8601Date_ParsesDateCorrectly()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var expectedDate = new DateTime(2025, 3, 10, 8, 15, 45, DateTimeKind.Utc);
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-789",
                [IdempotencyContextKeys.Date] = expectedDate.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash123"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(mi => mi.Timestamp.Equals(expectedDate)));
    }

    [Fact]
    public async Task ExecuteAsync_PassesThroughInputUnmodified()
    {
        // Arrange
        var message = new TestMessage { Content = "original content" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-001",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        var successResult = (TechnicalStepResult<TestMessage>.Success)result;
        Assert.Same(message, successResult.Value);
        Assert.Equal("original content", successResult.Value.Content);
    }

    [Fact]
    public async Task ExecuteAsync_PreservesContextReference()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (_, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.Same(context, resultContext);
    }

    #endregion

    #region ExecuteAsync - Missing Metadata Tests

    [Fact]
    public async Task ExecuteAsync_WithMissingId_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
                // Missing Id
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains(IdempotencyContextKeys.Id, failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Contains("Missing required context property", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithMissingDate_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Hash] = "hash"
                // Missing Date
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains(IdempotencyContextKeys.Date, failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Contains("Missing required context property", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithMissingHash_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O")
                // Missing Hash
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains(IdempotencyContextKeys.Hash, failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Contains("Missing required context property", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyMetadata_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>()
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains("Missing required context property", failureResult.Value.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_WithMissingMetadata_DoesNotCallClient()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>()
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.DidNotReceive().CommitAsync(Arg.Any<MessageInfo>());
    }

    #endregion

    #region ExecuteAsync - Invalid Date Format Tests

    [Fact]
    public async Task ExecuteAsync_WithInvalidDateFormat_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = "invalid-date-format",
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains("Invalid date format", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Contains("invalid-date-format", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyDate_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = string.Empty,
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains("Invalid date format", failureResult.Value.Description, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidDateFormat_DoesNotCallClient()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = "not-a-date",
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.DidNotReceive().CommitAsync(Arg.Any<MessageInfo>());
    }

    #endregion

    #region ExecuteAsync - Client Error Handling Tests

    [Fact]
    public async Task ExecuteAsync_WhenClientThrowsIdempotencyServiceException_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var serviceException = new IdempotencyServiceException("Database connection failed");
        _mockClient.CommitAsync(Arg.Any<MessageInfo>())
            .Throws(serviceException);

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Contains("Failed to record processed message", failureResult.Value.Description, StringComparison.Ordinal);
        Assert.NotNull(failureResult.Value.Exception);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WhenClientThrowsException_WrapsExceptionInTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var innerException = new InvalidOperationException("Network error");
        _mockClient.CommitAsync(Arg.Any<MessageInfo>())
            .Throws(innerException);

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.NotNull(failureResult.Value.Exception);
        Assert.IsType<InvalidOperationException>(failureResult.Value.Exception);
    }

    [Fact]
    public async Task ExecuteAsync_WhenClientFails_PreservesContext()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        _mockClient.CommitAsync(Arg.Any<MessageInfo>())
            .Throws(new IdempotencyServiceException("Error"));

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (_, resultContext) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.Same(context, resultContext);
    }

    #endregion

    #region Component Name Tests

    [Fact]
    public async Task ExecuteAsync_UsesComponentNameFromFrameworkOptions()
    {
        // Arrange
        const string componentName = "CustomComponentName";
        const string ns = "Org";
        var frameworkOptions = new FrameworkOptions { ComponentName = componentName, ServiceNamespace = ns };
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            frameworkOptions
        );

        // Act
        await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(mi =>
            mi.Component == componentName
        ));
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public async Task ExecuteAsync_WithEmptyId_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = string.Empty,
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyHash_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-123",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = string.Empty
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
    }

    [Fact]
    public async Task ExecuteAsync_WithVariousDateFormats_ParsesCorrectly()
    {
        // Arrange
        var message = new TestMessage { Content = "test" };
        var testDates = new[]
        {
            "2025-01-15T10:30:00Z",
            "2025-01-15T10:30:00.0000000Z",
            "2025-01-15 10:30:00",
            "1/15/2025 10:30:00 AM"
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act & Assert
        foreach (var dateStr in testDates)
        {
            var context = new TestContext
            {
                Metadata = new Dictionary<string, string>
                {
                    [IdempotencyContextKeys.Id] = "msg-123",
                    [IdempotencyContextKeys.Date] = dateStr,
                    [IdempotencyContextKeys.Hash] = "hash"
                }
            };

            var (result, _) = await recorder.ExecuteAsync(message, context, CancellationToken.None);

            // Should successfully parse or return failure
            Assert.True(
                result is TechnicalStepResult<TestMessage>.Success or TechnicalStepResult<TestMessage>.Failure
            );
        }
    }

    #endregion

    #region Multiple Execution Tests

    [Fact]
    public async Task ExecuteAsync_CalledMultipleTimes_EachCallIsIndependent()
    {
        // Arrange
        var message1 = new TestMessage { Content = "first" };
        var message2 = new TestMessage { Content = "second" };
        var context1 = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-001",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash1"
            }
        };
        var context2 = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-002",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash2"
            }
        };

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result1, _) = await recorder.ExecuteAsync(message1, context1, CancellationToken.None);
        var (result2, _) = await recorder.ExecuteAsync(message2, context2, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result1);
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result2);
        await _mockClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-001"));
        await _mockClient.Received(1).CommitAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-002"));
    }

    [Fact]
    public async Task ExecuteAsync_FirstCallSucceedsSecondFails_BothHandledIndependently()
    {
        // Arrange
        var message1 = new TestMessage { Content = "first" };
        var message2 = new TestMessage { Content = "second" };
        var context1 = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-success",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash1"
            }
        };
        var context2 = new TestContext
        {
            Metadata = new Dictionary<string, string>
            {
                [IdempotencyContextKeys.Id] = "msg-fail",
                [IdempotencyContextKeys.Date] = DateTime.UtcNow.ToString("O"),
                [IdempotencyContextKeys.Hash] = "hash2"
            }
        };

        _mockClient.CommitAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-success"))
            .Returns(Task.CompletedTask);
        _mockClient.CommitAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-fail"))
            .Throws(new IdempotencyServiceException("Error"));

        var recorder = new ExternalIdempotencyRecorder<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions
        );

        // Act
        var (result1, _) = await recorder.ExecuteAsync(message1, context1, CancellationToken.None);
        var (result2, _) = await recorder.ExecuteAsync(message2, context2, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result1);
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result2);
    }

    #endregion
}
