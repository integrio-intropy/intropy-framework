using System.Net;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Extractor;

public class ExtractorTests
{
    private const string ContextKeyCustomerId = "customer-id";

    private readonly IIdempotencyServiceClient _idempotencyServiceClient;
    private readonly IBusinessIncidentServiceClient _businessIncidentServiceClient;
    private readonly IServiceCollection _serviceCollection;
    private readonly HttpClient _serviceInvocationClient;

    public ExtractorTests()
    {
        _idempotencyServiceClient = Substitute.For<IIdempotencyServiceClient>();
        _businessIncidentServiceClient = Substitute.For<IBusinessIncidentServiceClient>();
        var daprClient = Substitute.For<DaprClient>();

        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        // Set up DaprClient for service invocation
        daprClient.CreateInvokeMethodRequest(Arg.Any<HttpMethod>(), Arg.Any<string>(), Arg.Any<string>())
            .ReturnsForAnyArgs(_ => new HttpRequestMessage());

        _serviceInvocationClient = Substitute.For<HttpClient>();
        _serviceInvocationClient.SendAsync(Arg.Any<HttpRequestMessage>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

        _serviceCollection = new ServiceCollection();
        _serviceCollection.AddSingleton(_idempotencyServiceClient);
        _serviceCollection.AddSingleton(_businessIncidentServiceClient);
        _serviceCollection.AddSingleton(daprClient);
        _serviceCollection.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "Test";
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
        const string customerId = "1337";
        var context = GetInitialContext(customerId);
        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act    
        var (result, finalContext) = await pipeline.Execute(customerId, context);

        // Assert 
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        AssertIdempotencyContextExists(finalContext);
    }

    [Fact]
    public async Task Execute_WithDuplicateMessage_SkipsProcessing()
    {
        // Arrange
        const string customerId = "1337";
        var context = GetInitialContext(customerId);
        _idempotencyServiceClient.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Ignore, Reason.SameData)));

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act    
        var (result, _) = await pipeline.Execute(customerId, context);

        // Assert 
        await _idempotencyServiceClient.ReceivedWithAnyArgs(1).GetStatusAsync(Arg.Any<MessageInfo>());
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.DidNotReceiveWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<CloudEvent>.Cancelled>(result);
    }

    [Fact]
    public async Task Execute_WithInvalidInput_RoutesBusinessIncident()
    {
        // Arrange
        const string
            customerId =
                "abc"; // string that should fail validation                                                                                     
        var context = GetInitialContext(customerId);
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ReturnsForAnyArgs(Task.CompletedTask);

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act    
        var (result, _) = await pipeline.Execute(customerId, context);

        // Assert 
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.ReceivedWithAnyArgs().Trigger(Arg.Any<Uri>(),
            Arg.Is<string>(x => x.Equals(customerId, StringComparison.Ordinal)),
            Arg.Is<string>(x => x.Equals(customerId, StringComparison.Ordinal)),
            Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<CloudEvent>.Success>(result);
    }

    [Fact]
    public async Task Execute_WithInvalidInput_ResultsInTechnicalFailure_WhenBusinessIncidentServiceClientThrows()
    {
        // Arrange
        const string
            customerId =
                "abc"; // string that should fail validation                                                                                     
        var context = GetInitialContext(customerId);
        _businessIncidentServiceClient.Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
                Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>())
            .ThrowsAsyncForAnyArgs(new BusinessIncidentServiceException("", new HttpRequestException()));

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider()).Build();

        // Act    
        var (result, _) = await pipeline.Execute(customerId, context);

        // Assert 
        await _idempotencyServiceClient.DidNotReceiveWithAnyArgs().CommitAsync(Arg.Any<MessageInfo>());
        await _businessIncidentServiceClient.ReceivedWithAnyArgs().Trigger(Arg.Any<Uri>(), Arg.Any<string>(),
            Arg.Any<string>(), Arg.Any<BusinessIncidentData>(), Arg.Any<string?>());
        Assert.IsType<StepResult<CloudEvent>.TechnicalFailure>(result);
    }

    [Fact]
    public async Task Execute_WithCustomHashFunction_UsesCustomHash()
    {
        // Arrange
        const string customHash = "custom-hash-123";
        const string customerId = "1337";
        var context = GetInitialContext(customerId);

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider())
            .WithIdempotency(
                (input, _) => input.CustomerId,
                (input, _) => input.EventDate,
                _ => customHash)
            .Build();

        // Act
        var (result, finalContext) = await pipeline.Execute(customerId, context);

        // Assert                       
        Assert.IsType<StepResult<CloudEvent>.Success>(result);
        Assert.Equal(customHash, finalContext.Metadata[IdempotencyContextKeys.Hash]);
    }

    [Fact]
    public async Task Execute_WithCloudEventSerializeStep_ProducesEquivalentEnvelope()
    {
        // Arrange - register the reusable step through the existing WithSerializer overload
        const string customerId = "1337";
        var context = GetInitialContext(customerId);

        var pipeline = GetPipelineBuilder(_serviceCollection.BuildServiceProvider())
            .WithSerializer(new CloudEventSerializeStep<CustomerOut, Context>(
                subject: c => c.CustomerId.ToString(),
                time: _ => DateTimeOffset.UtcNow))
            .Build();

        // Act
        var (result, _) = await pipeline.Execute(customerId, context);

        // Assert - envelope equivalent to the hand-rolled CustomerSerializer: same Subject
        // and Data, a fresh parseable Id, a set Time, and the default DataContentType
        var success = Assert.IsType<StepResult<CloudEvent>.Success>(result);
        var cloudEvent = success.Value;
        Assert.Equal(customerId, cloudEvent.Subject);
        Assert.Equal("application/json", cloudEvent.DataContentType);
        Assert.NotNull(cloudEvent.Time);
        Assert.NotEqual(default, cloudEvent.Time.Value);
        Assert.True(Guid.TryParse(cloudEvent.Id, out _));
        var data = Assert.IsType<CustomerOut>(cloudEvent.Data);
        Assert.Equal(1337, data.CustomerId);
    }

    [Fact]
    public async Task Execute_WithNoExtractSteps_ProcessesSuccessfully()
    {
        // Arrange
        const string customerId = "1337";
        var context = GetInitialContext(customerId);

        var pipeline = GetPipelineBuilderWithoutExtractors(_serviceCollection.BuildServiceProvider()).Build();

        // Act
        var (result, _) = await pipeline.Execute(customerId, context);

        // Assert 
        Assert.IsType<StepResult<CloudEvent>.Success>(result);
    }

    private static Context GetInitialContext(string customerId)
    {
        var metadata = new Dictionary<string, string> { { ContextKeyCustomerId, customerId } };
        return new Context(metadata);
    }

    private ExtractorBuilder<CustomerIn, CustomerOut, Context> GetPipelineBuilder(
        IServiceProvider serviceProvider)
    {
        return ExtractorBuilder<CustomerIn, CustomerOut, Context>.Create("Test", serviceProvider)
            .WithDeserializer(new CustomerDeserializer())
            .WithIdempotency(
                (input, _) => input.CustomerId,
                (input, _) => input.EventDate)
            .WithExtractor(new CustomerExtractor())
            .WithValidator(new CustomerValidator())
            .WithTransformer(new CustomerTransformer())
            .WithSerializer(new CustomerSerializer())
            .WithDaprServiceInvoker(
                appId: "reconciler",
                source: new Uri("urn:test:crm"),
                type: "com.test.customer.extracted",
                httpClientFactory: _ => _serviceInvocationClient)
            .WithBusinessIncidents(context => context.Metadata[ContextKeyCustomerId],
                context => context.Metadata[ContextKeyCustomerId]);
    }

    private ExtractorBuilder<CustomerIn, CustomerOut, Context> GetPipelineBuilderWithoutExtractors(
        IServiceProvider serviceProvider)
    {
        return ExtractorBuilder<CustomerIn, CustomerOut, Context>.Create("Test", serviceProvider)
            .WithDeserializer(new CustomerDeserializer())
            .WithIdempotency(
                (input, _) => input.CustomerId,
                (input, _) => input.EventDate)
            // No extractors - they will be added by individual tests
            .WithValidator(new CustomerValidator())
            .WithTransformer(new CustomerTransformer())
            .WithSerializer(new CustomerSerializer())
            .WithDaprServiceInvoker(
                appId: "reconciler",
                source: new Uri("urn:test:crm"),
                type: "com.test.customer.extracted",
                httpClientFactory: _ => _serviceInvocationClient)
            .WithBusinessIncidents(context => context.Metadata[ContextKeyCustomerId],
                context => context.Metadata[ContextKeyCustomerId]);
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK));
    }

    private static void AssertIdempotencyContextExists(Context context)
    {
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Id));
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Date));
        Assert.True(context.Metadata.ContainsKey(IdempotencyContextKeys.Hash));
    }
}
