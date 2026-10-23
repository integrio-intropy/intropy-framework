using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Blocks.Test.Loader;

/// <summary>
/// The idempotency id a loader checks and commits: the subject for an unrouted loader, scoped by
/// the route's event type by default in a routing loader, and the bare subject again when a route
/// opts into the entity scope.
/// </summary>
public class LoaderIdempotencyScopeTests
{
    private const string EventType = "com.test.customer.loaded";
    private readonly IIdempotencyServiceClient _idempotency = Substitute.For<IIdempotencyServiceClient>();
    private readonly IServiceProvider _provider;

    public LoaderIdempotencyScopeTests()
    {
        _idempotency.GetStatusAsync(Arg.Any<MessageInfo>())
            .ReturnsForAnyArgs(Task.FromResult(new StatusResponse(Action.Proceed, Reason.NoPreviousData)));

        var services = new ServiceCollection();
        services.AddSingleton(_idempotency);
        services.AddSingleton(Substitute.For<IBusinessIncidentServiceClient>());
        services.AddSingleton(Substitute.For<ILoggerFactory>());
        services.AddIntropyFramework(conf =>
        {
            conf.ComponentName = "LoaderTest";
            conf.ServiceNamespace = "Org";
        });
        _provider = services.BuildServiceProvider();
    }

    [Fact]
    public async Task Execute_WithoutRoute_KeysOnTheSubject()
    {
        var builder = CreateBuilder(routeEventType: null).WithIdempotency();

        await Execute(builder);

        await AssertIdAsync("customer-1337");
    }

    [Fact]
    public async Task Execute_OnRouteWithDefaultScope_KeysOnEventTypeAndSubject()
    {
        var builder = CreateBuilder(EventType).WithIdempotency();

        await Execute(builder);

        await AssertIdAsync($"{EventType}:customer-1337");
    }

    [Fact]
    public async Task Execute_OnRouteWithEntityScope_KeysOnTheSubject()
    {
        var builder = CreateBuilder(EventType).WithIdempotency(scope: IdempotencyScope.Entity);

        await Execute(builder);

        await AssertIdAsync("customer-1337");
    }

    [Fact]
    public async Task Execute_WithIdExtractor_KeysOnTheExtractedId()
    {
        var builder = CreateBuilder(routeEventType: null)
            .WithIdempotency(idExtractor: (input, _) => $"customer-id-{input.CustomerId}");

        await Execute(builder);

        await AssertIdAsync("customer-id-1337");
    }

    [Fact]
    public async Task Execute_WithIdExtractorOnRouteWithDefaultScope_KeysOnEventTypeAndExtractedId()
    {
        var builder = CreateBuilder(EventType)
            .WithIdempotency(idExtractor: (input, _) => $"customer-id-{input.CustomerId}");

        await Execute(builder);

        await AssertIdAsync($"{EventType}:customer-id-1337");
    }

    [Fact]
    public async Task Execute_WithIdExtractor_CanReadTheContext()
    {
        var builder = CreateBuilder(routeEventType: null)
            .WithIdempotency(idExtractor: (_, context) => context.Metadata[CloudEventContextKeys.Source]);

        await Execute(builder);

        await AssertIdAsync("urn:test:source");
    }

    [Fact]
    public async Task Execute_WithIdExtractorReturningEmpty_FailsTechnically()
    {
        var builder = CreateBuilder(routeEventType: null).WithIdempotency(idExtractor: (_, _) => "");

        var result = await Execute(builder);

        Assert.IsType<StepResult<LoaderCustomerOut>.TechnicalFailure>(result);
        await _idempotency.DidNotReceiveWithAnyArgs().GetStatusAsync(default!);
    }

    private LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context> CreateBuilder(string? routeEventType)
    {
        var builder = routeEventType is null
            ? LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>.Create("LoaderTest", _provider)
            : LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context>.Create("LoaderTest", _provider, routeEventType);
        return builder
            .WithDeserializer(new CustomerDeserializer())
            .WithValidator(new CustomerValidator())
            .WithTransformer(new CustomerTransformer())
            .WithSender(new CustomerSender())
            .WithBusinessIncidents(_ => "unknown", _ => "unknown");
    }

    private static async Task<StepResult<LoaderCustomerOut>> Execute(LoaderBuilder<LoaderCustomerIn, LoaderCustomerOut, Context> builder)
    {
        var cloudEvent = new CloudEvent
        {
            Id = Guid.NewGuid().ToString(),
            Subject = "customer-1337",
            Time = DateTimeOffset.UtcNow,
            Source = new Uri("urn:test:source"),
            Type = EventType,
            Data = new LoaderCustomerIn { CustomerId = "1337", Name = "John Doe" },
            DataContentType = "application/json"
        };
        var (result, _) = await builder.Build().Execute(cloudEvent, new Context(new Dictionary<string, string>()));
        return result;
    }

    private async Task AssertIdAsync(string expectedId)
    {
        await _idempotency.Received(1).GetStatusAsync(Arg.Is<MessageInfo>(m => m.Id == expectedId));
        await _idempotency.Received(1).CommitAsync(Arg.Is<MessageInfo>(m => m.Id == expectedId));
    }
}
