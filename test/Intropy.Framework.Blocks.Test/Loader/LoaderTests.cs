using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Loader;

public class LoaderTests
{
    private readonly IIdempotencyServiceClient _idempotencyServiceClient;
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly IServiceCollection _serviceCollection;

    public LoaderTests()
    {
        _idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        _businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();

        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        _serviceCollection = new ServiceCollection();
        _serviceCollection.AddSingleton(_idempotencyServiceClient);
        _serviceCollection.AddSingleton(_businessIncidentServiceClient);
        _serviceCollection.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "LoaderTest";
            conf.ServiceNamespace = "Org";
        });

        var mockLoggerFactory = Substitute.For<ILoggerFactory>();
        var mockLogger = Substitute.For<ILogger>();
        mockLoggerFactory.CreateLogger(Arg.Any<string>()).Returns(mockLogger);
        _serviceCollection.AddSingleton(mockLoggerFactory);
    }

    [Fact]
    public async Task Execute_WithValidInput_ProcessesSuccessfully()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, finalContext) = await pipeline.Execute(cloudEvent, context);

        // Assert
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
        AssertIdempotencyContextExists(finalContext);
    }

    [Fact]
    public async Task Execute_WithDuplicateMessage_SkipsProcessing()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();
        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Ignore, Reason.SameData)));

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<LoaderCustomerOut>.Cancelled>(result);
    }

    [Fact]
    public async Task Execute_WithInvalidInput_RoutesBusinessIncident()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("invalid-id", "John Doe"); // Non-numeric ID
        var context = GetInitialContext();
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.ReceivedWithAnyArgs()
            .Trigger(Arg.Any<Uri>(), Arg.Is<string>(x => x.Equals("customer-invalid-id", StringComparison.Ordinal)),
                Arg.Is<string>(x => x.Equals("customer-invalid-id", StringComparison.Ordinal)),
                Arg.Any<BusinessIncidentData>(),
                Arg.Any<string?>());
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(result);
    }

    [Fact]
    public async Task Execute_WithInvalidInput_ResultsInTechnicalFailure_WhenBusinessIncidentServiceClientThrows()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("invalid-id", "John Doe"); // Non-numeric ID
        var context = GetInitialContext();
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ThrowsAsyncForAnyArgs(new BusinessIncidentServiceException("", new HttpRequestException()));

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.ReceivedWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Execute_WithCustomHashFunction_UsesCustomHash()
    {
        // Arrange
        const string customHash = "custom-hash-123";
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider())
            .WithIdempotency(_ => customHash)
            .Build();

        // Act
        var (result, finalContext) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
        Assert.Equal(customHash, finalContext.Metadata[IdempotencyContextKeys.Hash]);
    }

    [Fact]
    public async Task Execute_ExtractsCloudEventMetadataToContext()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, finalContext) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
        Assert.True(finalContext.Metadata.ContainsKey(CloudEventContextKeys.Id));
        Assert.True(finalContext.Metadata.ContainsKey(CloudEventContextKeys.Subject));
        Assert.True(finalContext.Metadata.ContainsKey(CloudEventContextKeys.Time));
        Assert.True(finalContext.Metadata.ContainsKey(CloudEventContextKeys.Source));
        Assert.True(finalContext.Metadata.ContainsKey(CloudEventContextKeys.Type));
        Assert.Equal(cloudEvent.Id, finalContext.Metadata[CloudEventContextKeys.Id]);
        Assert.Equal(cloudEvent.Subject, finalContext.Metadata[CloudEventContextKeys.Subject]);
    }

    [Fact]
    public async Task Execute_WithMissingCloudEventSubject_ResultsInTechnicalFailure()
    {
        // Arrange
        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            // Subject is missing
            Time = DateTimeOffset.UtcNow,
            Source = new Uri("urn:test:source"),
            Type = "com.test.customer.loaded",
            Data = new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" },
            DataContentType = "application/json"
        };
        var context = GetInitialContext();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Execute_WithMissingCloudEventTime_ResultsInTechnicalFailure()
    {
        // Arrange
        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Subject = "customer-1337",
            // Time is missing
            Source = new Uri("urn:test:source"),
            Type = "com.test.customer.loaded",
            Data = new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" },
            DataContentType = "application/json"
        };
        var context = GetInitialContext();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Execute_WithReceiptSender_SendsReceiptOnSuccess()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();
        var receiptSender = new ReceiptSender();

        var pipeline = GetPipelineWithReceiptSender(
            _serviceCollection.BuildServiceProvider(), receiptSender);

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
        var receipt = Assert.Single(receiptSender.SentReceipts);
        Assert.Equal(1337, receipt.CustomerId);
    }

    [Fact]
    public async Task Execute_WithReceiptSender_DoesNotSendOnBusinessFailure()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("invalid-id", "John Doe");
        var context = GetInitialContext();
        var receiptSender = new ReceiptSender();
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var pipeline = GetPipelineWithReceiptSender(
            _serviceCollection.BuildServiceProvider(), receiptSender);

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(result);
        Assert.Empty(receiptSender.SentReceipts);
    }

    [Fact]
    public async Task Execute_WithReceiptSender_ResultsInTechnicalFailure_WhenSendFails()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var context = GetInitialContext();

        var pipeline = GetPipelineWithReceiptSender(
            _serviceCollection.BuildServiceProvider(), new FailingReceiptSender());

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, context);

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Execute_CompletePipeline_WithAllFeatures()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "  John Doe  ");
        var context = GetInitialContext();
        var receiptSender = new ReceiptSender();

        var pipeline = LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>
            .Create("LoaderTest", _serviceCollection.BuildServiceProvider())
            .WithDeserializer(new CustomerDeserializer())
            .WithExtractor(new CustomerNameNormalizer())
            .WithValidator(new CustomerValidator())
            .WithIdempotency(input => $"{input.CustomerId}|{input.Name}")
            .WithTransformer(new CustomerTransformer())
            .WithSender(new CustomerSender())
            .WithBusinessIncidents(ctx =>
                    ctx.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"))
            .WithReceiptSender(receiptSender)
            .Build();

        // Act
        var (result, finalContext) = await pipeline.Execute(cloudEvent, context);

        // Assert — pipeline completed successfully
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);

        // Assert — CloudEvent metadata was extracted to context
        Assert.Equal(cloudEvent.Id, finalContext.Metadata[CloudEventContextKeys.Id]);
        Assert.Equal(cloudEvent.Subject, finalContext.Metadata[CloudEventContextKeys.Subject]);
        Assert.NotNull(finalContext.Metadata[CloudEventContextKeys.Time]);
        Assert.NotNull(finalContext.Metadata[CloudEventContextKeys.Source]);
        Assert.NotNull(finalContext.Metadata[CloudEventContextKeys.Type]);

        // Assert — custom idempotency hash reflects extracted (normalized) input
        Assert.Equal("1337|JOHN DOE", finalContext.Metadata[IdempotencyContextKeys.Hash]);

        // Assert — idempotency was checked and recorded
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).CommitAsync(Arg.Any<MessageInfo>());

        // Assert — receipt was sent with the transformed output
        var receipt = Assert.Single(receiptSender.SentReceipts);
        Assert.Equal(1337, receipt.CustomerId);

        // Assert — no business incidents were routed
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs()
            .Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Execute_WithoutIdempotency_LoadsAndNeverCallsTheIdempotencyService()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var sender = new CustomerSender();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider(), withIdempotency: false,
            sender: sender).Build();

        // Act - the same message twice: without idempotency neither is a duplicate
        var (first, firstContext) = await pipeline.Execute(cloudEvent, GetInitialContext());
        var (second, _) = await pipeline.Execute(cloudEvent, GetInitialContext());

        // Assert
        Assert.Empty(_idempotencyServiceClient.ReceivedCalls());
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(first);
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(second);
        Assert.Equal(2, sender.SentCount);
        Assert.False(firstContext.Metadata.ContainsKey(IdempotencyContextKeys.Id));
        Assert.False(firstContext.Metadata.ContainsKey(IdempotencyContextKeys.Hash));
    }

    [Fact]
    public async Task Execute_WithoutIdempotency_SendsReceiptOnSuccess()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        var receiptSender = new ReceiptSender();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider(), withIdempotency: false)
            .WithReceiptSender(receiptSender)
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, GetInitialContext());

        // Assert - the receipt follows the load even with no idempotency record in between
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
        var receipt = Assert.Single(receiptSender.SentReceipts);
        Assert.Equal(1337, receipt.CustomerId);
        Assert.Empty(_idempotencyServiceClient.ReceivedCalls());
    }

    [Fact]
    public async Task Execute_WithoutIdempotency_StillRoutesBusinessIncidents()
    {
        // Arrange
        var cloudEvent = CreateCloudEvent("invalid-id", "John Doe"); // Non-numeric ID
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);
        var receiptSender = new ReceiptSender();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider(), withIdempotency: false)
            .WithReceiptSender(receiptSender)
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, GetInitialContext());

        // Assert
        Assert.Empty(_idempotencyServiceClient.ReceivedCalls());
        await _businessIncidentServiceClient.ReceivedWithAnyArgs(1).Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.Empty(receiptSender.SentReceipts);
        Assert.IsType<StepResult<LoaderCustomerOut>.RoutedIncident>(result);
    }

    [Fact]
    public async Task Execute_WithoutIdempotency_NeedsNoCloudEventSubjectOrTime()
    {
        // Arrange - a message without the attributes idempotency would key on
        var cloudEvent = CreateCloudEvent("1337", "John Doe");
        cloudEvent.Subject = null;
        cloudEvent.Time = null;
        _serviceCollection.RemoveAll<IIdempotencyServiceClient>();
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider(), withIdempotency: false).Build();

        // Act
        var (result, _) = await pipeline.Execute(cloudEvent, GetInitialContext());

        // Assert
        Assert.IsType<StepResult<LoaderCustomerOut>.Success>(result);
    }

    private static CloudEvent CreateCloudEvent(string customerId, string name)
    {
        return new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Subject = $"customer-{customerId}",
            Time = DateTimeOffset.UtcNow,
            Source = new Uri("urn:test:source"),
            Type = "com.test.customer.loaded",
            Data = new LoaderCustomerIn { CustomerId = customerId, Name = name },
            DataContentType = "application/json"
        };
    }

    private static Context GetInitialContext()
    {
        var metadata = new Dictionary<string, string>();
        return new Context(metadata);
    }

    private static LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context> GetPipelineBuilder(
        IServiceProvider serviceProvider, bool withIdempotency = true, CustomerSender? sender = null)
    {
        var builder = LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>.Create("LoaderTest", serviceProvider)
            .WithDeserializer(new CustomerDeserializer())
            .WithValidator(new CustomerValidator());
        if (withIdempotency)
            builder.WithIdempotency();
        return builder
            .WithTransformer(new CustomerTransformer())
            .WithSender(sender ?? new CustomerSender())
            .WithBusinessIncidents(context =>
                    context.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"),
                context => context.Metadata.GetValueOrDefault(CloudEventContextKeys.Subject, "unknown"));
    }

    private static Loader<LoaderCustomerIn, LoaderCustomerOut, Context> GetPipelineWithReceiptSender(
        IServiceProvider serviceProvider, SendStep<LoaderCustomerOut, Context> receiptSender)
    {
        return GetPipelineBuilder(serviceProvider)
            .WithReceiptSender(receiptSender)
            .Build();
    }

    private static void AssertIdempotencyContextExists(Context context)
    {
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Id));
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Date));
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Hash));
    }
}
