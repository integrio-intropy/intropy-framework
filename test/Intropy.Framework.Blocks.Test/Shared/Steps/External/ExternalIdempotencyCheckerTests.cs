using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using JetBrains.Annotations;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Shared.Steps.External;

public class ExternalIdempotencyCheckerTests
{
    // Test types
    private sealed class TestMessage
    {
        public string Id { get; init; } = string.Empty;
        public DateTimeOffset EventDate { get; init; }
        public string Content { [UsedImplicitly] get; set; } = string.Empty;
    }

    private sealed record TestContext() : Context(new Dictionary<string, string>());

    private sealed class HashableMessage : IHashable
    {
        public string Id { get; init; } = string.Empty;
        public DateTime EventDate { get; init; }
        public string Data { get; init; } = string.Empty;

        public string GetHashString() => $"{Id}:{Data}";
    }

    // Test fixtures
    private readonly IIdempotencyServiceClient _mockClient;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly Func<TestMessage, TestContext, string> _idExtractor;
    private readonly Func<TestMessage, TestContext, DateTimeOffset> _dateExtractor;

    public ExternalIdempotencyCheckerTests()
    {
        _mockClient = Substitute.For<IIdempotencyServiceClient>();
        _frameworkOptions = new FrameworkOptions { ComponentName = "TestComponent", ServiceNamespace = "TestNamespace" };
        _idExtractor = (msg, _) => msg.Id;
        _dateExtractor = (msg, _) => msg.EventDate;
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithAllValidParameters_CreatesInstance()
    {
        // Act
        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Assert
        Assert.NotNull(checker);
    }

    [Fact]
    public void Constructor_WithNullClient_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyChecker<TestMessage, TestContext>(
                null!,
                _frameworkOptions,
                _idExtractor,
                _dateExtractor
            )
        );

        Assert.Equal("client", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullFrameworkOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyChecker<TestMessage, TestContext>(
                _mockClient,
                null!,
                _idExtractor,
                _dateExtractor
            )
        );

        Assert.Equal("frameworkOptions", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullIdExtractor_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyChecker<TestMessage, TestContext>(
                _mockClient,
                _frameworkOptions,
                null!,
                _dateExtractor
            )
        );

        Assert.Equal("idExtractor", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullDateExtractor_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() =>
            new ExternalIdempotencyChecker<TestMessage, TestContext>(
                _mockClient,
                _frameworkOptions,
                _idExtractor,
                null!
            )
        );

        Assert.Equal("dateExtractor", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullHashGenerator_UsesDefaultHashGenerator()
    {
        // Act
        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor,
            hashGenerator: null
        );

        // Assert
        Assert.NotNull(checker);
    }

    #endregion

    #region ExecuteAsync - Success Path Tests

    [Fact]
    public async Task ExecuteAsync_WhenMessageIsNew_ReturnsSuccessAndStoresMetadata()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-123",
            EventDate = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            Content = "test content"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result, resultContext) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result);
        var successResult = (TechnicalStepResult<TestMessage>.Success)result;
        Assert.Same(message, successResult.Value);
        Assert.Same(context, resultContext);

        // Verify metadata was stored
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Id));
        Assert.Equal("msg-123", context.Metadata[IdempotencyContextKeys.Id]);
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Date));
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Hash));
    }

    [Fact]
    public async Task ExecuteAsync_WhenMessageIsNew_StoresDateInIso8601Format()
    {
        // Arrange
        var eventDate = new DateTimeOffset(new DateTime(2025, 1, 15, 10, 30, 45, DateTimeKind.Utc));
        var message = new TestMessage
        {
            Id = "msg-123",
            EventDate = eventDate,
            Content = "test"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        var storedDate = context.Metadata[IdempotencyContextKeys.Date];
        Assert.Equal(eventDate.ToString("O"), storedDate);
    }

    [Fact]
    public async Task ExecuteAsync_CallsIdempotencyServiceWithCorrectParameters()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-456",
            EventDate = new DateTime(2025, 2, 20, 15, 45, 0, DateTimeKind.Utc),
            Content = "specific content"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(mi =>
            mi.Component == "TestComponent" &&
            mi.Id == "msg-456" &&
            mi.Timestamp == message.EventDate &&
            !string.IsNullOrEmpty(mi.Hash)
        ));
    }

    #endregion

    #region ExecuteAsync - Duplicate/Ignore Tests

    [Fact]
    public async Task ExecuteAsync_WhenMessageIsDuplicate_ReturnsCancelledWithoutStoringMetadata()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "duplicate-msg",
            EventDate = DateTime.UtcNow,
            Content = "duplicate"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Ignore, Reason.SameData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result, resultContext) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Cancelled>(result);
        Assert.Same(context, resultContext);
        Assert.Empty(context.Metadata);
    }

    #endregion

    #region ExecuteAsync - Error Handling Tests

    [Fact]
    public async Task ExecuteAsync_WhenIdempotencyServiceThrowsException_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-789",
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext();

        var serviceException = new IdempotencyServiceException("Service unavailable");
        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Throws(serviceException);

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result, resultContext) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
        var failureResult = (TechnicalStepResult<TestMessage>.Failure)result;
        Assert.Equal("Failed to check idempotency", failureResult.Value.Description);
        Assert.Same(serviceException, failureResult.Value.Exception);
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WhenIdempotencyServiceFails_DoesNotStoreMetadata()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-fail",
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Throws(new IdempotencyServiceException("Database error"));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.Empty(context.Metadata);
    }

    #endregion

    #region Custom Hash Generator Tests

    [Fact]
    public async Task ExecuteAsync_WithCustomHashGenerator_UsesProvidedHashFunction()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-custom",
            EventDate = DateTime.UtcNow,
            Content = "custom hash test"
        };
        var context = new TestContext();
        const string customHash = "CUSTOM-HASH-VALUE";

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NewerData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor,
            hashGenerator: _ => customHash
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.Equal(customHash, context.Metadata[IdempotencyContextKeys.Hash]);
        await _mockClient.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(mi =>
            mi.Hash == customHash
        ));
    }

    [Fact]
    public async Task ExecuteAsync_WithDefaultHashGenerator_GeneratesConsistentHash()
    {
        // Arrange
        var message1 = new TestMessage
        {
            Id = "msg-001",
            EventDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            Content = "same content"
        };
        var message2 = new TestMessage
        {
            Id = "msg-001",
            EventDate = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            Content = "same content"
        };
        var context1 = new TestContext();
        var context2 = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NewerData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message1, context1, CancellationToken.None);
        await checker.ExecuteAsync(message2, context2, CancellationToken.None);

        // Assert
        Assert.Equal(context1.Metadata[IdempotencyContextKeys.Hash], 
                    context2.Metadata[IdempotencyContextKeys.Hash]);
    }

    [Fact]
    public async Task ExecuteAsync_WithDefaultHashGenerator_GeneratesDifferentHashForDifferentContent()
    {
        // Arrange
        var message1 = new TestMessage
        {
            Id = "msg-001",
            EventDate = DateTime.UtcNow,
            Content = "content A"
        };
        var message2 = new TestMessage
        {
            Id = "msg-001",
            EventDate = DateTime.UtcNow,
            Content = "content B"
        };
        var context1 = new TestContext();
        var context2 = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NewerData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message1, context1, CancellationToken.None);
        await checker.ExecuteAsync(message2, context2, CancellationToken.None);

        // Assert
        Assert.NotEqual(context1.Metadata[IdempotencyContextKeys.Hash], 
                       context2.Metadata[IdempotencyContextKeys.Hash]);
    }

    #endregion

    #region IHashable Implementation Tests

    [Fact]
    public async Task ExecuteAsync_WithIHashableMessage_UsesGetHashStringMethod()
    {
        // Arrange
        var message = new HashableMessage
        {
            Id = "hashable-msg",
            EventDate = DateTime.UtcNow,
            Data = "test-data"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NewerData));

        var checker = new ExternalIdempotencyChecker<HashableMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            IdExtractor,
            DateExtractor
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        // The hash should be based on GetHashString() output
        Assert.NotEmpty(context.Metadata[IdempotencyContextKeys.Hash]);
        // Verify the client was called with a hash
        await _mockClient.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(mi =>
            !string.IsNullOrEmpty(mi.Hash)
        ));
        return;

        DateTimeOffset DateExtractor(HashableMessage m, TestContext c) => m.EventDate;

        string IdExtractor(HashableMessage m, TestContext c) => m.Id;
    }

    #endregion

    #region Edge Cases and Validation Tests

    [Fact]
    public async Task ExecuteAsync_WithEmptyId_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = string.Empty,
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext();

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result, _) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
    }

    [Fact]
    public async Task ExecuteAsync_PreservesContextReference()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = "msg-123",
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext
        {
            Metadata = new Dictionary<string, string> { { "test", "test" } }
        };

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (_, resultContext) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.Same(context, resultContext);
    }

    [Fact]
    public async Task ExecuteAsync_WhenIdExtractorReturnsNull_ReturnsTechnicalFailure()
    {
        // Arrange
        var message = new TestMessage
        {
            Id = null!,
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext();

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result, _) = await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Failure>(result);
    }

    #endregion

    #region Component Name Tests

    [Fact]
    public async Task ExecuteAsync_UsesComponentNameFromFrameworkOptions()
    {
        // Arrange
        const string componentName = "MyCustomComponent";
        const string ns = "Org";
        var frameworkOptions = new FrameworkOptions { ComponentName = componentName, ServiceNamespace = ns };
        var message = new TestMessage
        {
            Id = "msg-123",
            EventDate = DateTime.UtcNow,
            Content = "test"
        };
        var context = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        await checker.ExecuteAsync(message, context, CancellationToken.None);

        // Assert
        await _mockClient.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(mi =>
            mi.Component == componentName
        ));
    }

    #endregion

    #region Multiple Execution Tests

    [Fact]
    public async Task ExecuteAsync_CalledMultipleTimes_EachCallGetsCorrectResult()
    {
        // Arrange
        var message1 = new TestMessage { Id = "msg-1", EventDate = DateTime.UtcNow, Content = "first" };
        var message2 = new TestMessage { Id = "msg-2", EventDate = DateTime.UtcNow, Content = "second" };
        var context1 = new TestContext();
        var context2 = new TestContext();

        _mockClient.GetStatusAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-1"))
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));
        _mockClient.GetStatusAsync(Arg.Is<MessageInfo>(mi => mi.Id == "msg-2"))
            .Returns(new StatusResponse(Action.Ignore, Reason.SameData));

        var checker = new ExternalIdempotencyChecker<TestMessage, TestContext>(
            _mockClient,
            _frameworkOptions,
            _idExtractor,
            _dateExtractor
        );

        // Act
        var (result1, _) = await checker.ExecuteAsync(message1, context1, CancellationToken.None);
        var (result2, _) = await checker.ExecuteAsync(message2, context2, CancellationToken.None);

        // Assert
        Assert.IsType<TechnicalStepResult<TestMessage>.Success>(result1);
        Assert.IsType<TechnicalStepResult<TestMessage>.Cancelled>(result2);
        Assert.NotEmpty(context1.Metadata);
        Assert.Empty(context2.Metadata);
    }

    #endregion

    #region Nested Object Hash Tests

    private sealed class SupplierWrapper
    {
        public string Id { get; init; } = string.Empty;
        public DateTime EventDate { get; init; }
        public List<SupplierDto> Suppliers { get; init; } = [];
    }

    private sealed class SupplierDto
    {
        public string FullRecord { get; init; } = string.Empty;
        public string ChangeStatus { get; init; } = string.Empty;
        public string AparGrId { get; init; } = string.Empty;
        public string AparId { get; init; } = string.Empty;
        public string AparIdRef { get; init; } = string.Empty;
        public string AparName { get; init; } = string.Empty;
        public string AparType { get; init; } = string.Empty;
        public string BankAccount { get; init; } = string.Empty;
        public string Client { get; init; } = string.Empty;
        public string CompRegNo { get; init; } = string.Empty;
        public string Currency { get; init; } = string.Empty;
        public string ExtAparRef { get; init; } = string.Empty;
        public string PayMethod { get; init; } = string.Empty;
        public string ShortName { get; init; } = string.Empty;
        public string Status { get; init; } = string.Empty;
        public string Address { get; init; } = string.Empty;
        public string CountryCode { get; init; } = string.Empty;
        public string Description { get; init; } = string.Empty;
        public string Place { get; init; } = string.Empty;
        public string ZipCode { get; init; } = string.Empty;
    }

    [Fact]
    public async Task ExecuteAsync_WithNestedObjectsInCollection_DifferentPropertyValues_GeneratesDifferentHashes()
    {
        // Arrange - Two payloads that differ only in AparType (empty vs "R")
        var payload1 = new SupplierWrapper
        {
            Id = "supplier-batch-1",
            EventDate = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            Suppliers =
            [
                new SupplierDto
                {
                    FullRecord = "1",
                    ChangeStatus = "I",
                    AparGrId = "B7",
                    AparId = "",
                    AparIdRef = "",
                    AparName = "Testperson Andersson",
                    AparType = "", // Empty in payload 1
                    BankAccount = "",
                    Client = "01",
                    CompRegNo = "197912292393",
                    Currency = "SEK",
                    ExtAparRef = "197912292393",
                    PayMethod = "IB",
                    ShortName = ".",
                    Status = "N",
                    Address = "c/o Test care of                        Fantasivägen 230",
                    CountryCode = "S",
                    Description = "E",
                    Place = "Karlstad",
                    ZipCode = "655 91"
                }
            ]
        };

        var payload2 = new SupplierWrapper
        {
            Id = "supplier-batch-1",
            EventDate = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            Suppliers =
            [
                new SupplierDto
                {
                    FullRecord = "1",
                    ChangeStatus = "I",
                    AparGrId = "B7",
                    AparId = "",
                    AparIdRef = "",
                    AparName = "Testperson Andersson",
                    AparType = "R", // "R" in payload 2 - THIS IS THE DIFFERENCE
                    BankAccount = "",
                    Client = "01",
                    CompRegNo = "197912292393",
                    Currency = "SEK",
                    ExtAparRef = "197912292393",
                    PayMethod = "IB",
                    ShortName = ".",
                    Status = "N",
                    Address = "c/o Test care of                        Fantasivägen 230",
                    CountryCode = "S",
                    Description = "E",
                    Place = "Karlstad",
                    ZipCode = "655 91"
                }
            ]
        };

        var context1 = new TestContext();
        var context2 = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<SupplierWrapper, TestContext>(
            _mockClient,
            _frameworkOptions,
            (msg, _) => msg.Id,
            (msg, _) => msg.EventDate
        );

        // Act
        await checker.ExecuteAsync(payload1, context1, CancellationToken.None);
        await checker.ExecuteAsync(payload2, context2, CancellationToken.None);

        // Assert - The hashes MUST be different since AparType differs
        var hash1 = context1.Metadata[IdempotencyContextKeys.Hash];
        var hash2 = context2.Metadata[IdempotencyContextKeys.Hash];

        Assert.NotEqual(hash1, hash2);
    }

    [Fact]
    public async Task ExecuteAsync_WithIdenticalNestedObjects_GeneratesSameHash()
    {
        // Arrange - Two identical payloads
        var payload1 = new SupplierWrapper
        {
            Id = "supplier-batch-1",
            EventDate = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            Suppliers =
            [
                new SupplierDto
                {
                    FullRecord = "1",
                    ChangeStatus = "I",
                    AparGrId = "B7",
                    AparName = "Testperson Andersson",
                    AparType = "R",
                    Client = "01",
                    CompRegNo = "197912292393"
                }
            ]
        };

        var payload2 = new SupplierWrapper
        {
            Id = "supplier-batch-1",
            EventDate = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc),
            Suppliers =
            [
                new SupplierDto
                {
                    FullRecord = "1",
                    ChangeStatus = "I",
                    AparGrId = "B7",
                    AparName = "Testperson Andersson",
                    AparType = "R",
                    Client = "01",
                    CompRegNo = "197912292393"
                }
            ]
        };

        var context1 = new TestContext();
        var context2 = new TestContext();

        _mockClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .Returns(new StatusResponse(Action.Proceed, Reason.NoPreviousData));

        var checker = new ExternalIdempotencyChecker<SupplierWrapper, TestContext>(
            _mockClient,
            _frameworkOptions,
            (msg, _) => msg.Id,
            (msg, _) => msg.EventDate
        );

        // Act
        await checker.ExecuteAsync(payload1, context1, CancellationToken.None);
        await checker.ExecuteAsync(payload2, context2, CancellationToken.None);

        // Assert - The hashes MUST be the same since payloads are identical
        var hash1 = context1.Metadata[IdempotencyContextKeys.Hash];
        var hash2 = context2.Metadata[IdempotencyContextKeys.Hash];

        Assert.Equal(hash1, hash2);
    }

    #endregion
}
