using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Gateway;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components.Test.Samples;

// The overriding and misdeclaring samples share OrderGateway's input and output, so they reuse its
// steps; the ones that name no steps have their own input types, so they never collide with the
// working samples' in the shared test assembly.

/// <summary>Overrides every default a gateway offers.</summary>
[IntropyComponent("custom-order-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:custom-order-gateway")]
public sealed class CustomOrderGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)
                .Incidents(new IncidentRule(_ => "custom-id", _ => "custom-subject"))
                // Extends the framework's pipeline instead of restating it.
                .Extend((builder, _) => builder.WithSerializer(
                    new CloudEventSerializeStep<OrderPlaced, Context>(o => $"custom:{o.OrderId}", o => o.OrderDate)))));

    protected override void ConfigureOptions(GatewayOptions options) => options.MaxRequestBodyBytes = 256;

    protected override IResult Respond(GatewayOutcome outcome) => outcome is GatewayOutcome.Published
        ? Results.StatusCode(StatusCodes.Status201Created)
        : GatewayResponses.Default(outcome);
}

/// <summary>Exposes the attributed endpoint at another path than the attribute's.</summary>
[IntropyComponent("moved-path-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:moved-path-gateway")]
public sealed class MovedPathGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(OrderGateway.Endpoint, "/elsewhere", AcceptsOrders);

    internal static void AcceptsOrders(GatewayEndpoint endpoint) =>
        endpoint.Accepts<SourceOrder, OrderPlaced>(route => route
            .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

/// <summary>Renames the attributed endpoint.</summary>
[IntropyComponent("renamed-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:renamed-endpoint-gateway")]
public sealed class RenamedEndpointGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint("orders-in", "/orders", MovedPathGateway.AcceptsOrders);
}

/// <summary>Declares an endpoint that accepts nothing.</summary>
[IntropyComponent("accepting-nothing-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:accepting-nothing-gateway")]
public sealed class AcceptingNothingGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(_ => { });
}

/// <summary>Has its endpoint accept two kinds of data.</summary>
[IntropyComponent("accepting-twice-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:accepting-twice-gateway")]
public sealed class AcceptingTwiceGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint =>
        {
            MovedPathGateway.AcceptsOrders(endpoint);
            endpoint.Accepts<EdiOrder, OrderPlaced>(route => route
                .Steps<EdiDeserializer, EdiValidator, EdiTransformer>()
                .Idempotency((o, _) => o.Reference, (o, _) => o.Issued)
                .OutputEvent(o => o.OrderId, o => o.OrderDate));
        });
}

/// <summary>Accepts orders without an idempotency rule.</summary>
[IntropyComponent("ruleless-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:ruleless-gateway")]
public sealed class RulelessGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .OutputEvent(o => o.OrderId, o => o.OrderDate)));
}

public sealed record StepLessInput(string Id);

/// <summary>Names no steps on a route that accepts.</summary>
[IntropyComponent("stepless-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:stepless-gateway")]
public sealed class StepLessGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<StepLessInput, OrderPlaced>(route => route
                .Idempotency((i, _) => i.Id, (_, _) => DateTimeOffset.UnixEpoch)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)));
}

/// <summary>Registers a deserializer slot in <c>ConfigureServices</c>: an author-registered slot
/// wins over the route's declared class.</summary>
[IntropyComponent("author-steps-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:author-steps-gateway")]
public sealed class AuthorStepsGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)));

    protected override void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(new Greeting("author"));
        services.AddTransient<DeserializeStep<SourceOrder, Context>, AuthorDeserializer>();
    }
}

/// <summary>A service only the author's deserializer takes, to pin constructor injection.</summary>
/// <param name="Prefix">What the author's deserializer prefixes the order id with.</param>
public sealed record Greeting(string Prefix);

/// <summary>The author's deserializer: prefixes the order id, so the published event names whose
/// deserializer ran.</summary>
public sealed class AuthorDeserializer(Greeting greeting) : DeserializeStep<SourceOrder, Context>
{
    private static readonly System.Text.Json.JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<SourceOrder> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var order = System.Text.Json.JsonSerializer.Deserialize<SourceOrder>(input.Span, Options)!;
        return Task.FromResult<(BusinessStepResult<SourceOrder>, Context)>(
            (new BusinessStepResult<SourceOrder>.Success(order with { OrderId = $"{greeting.Prefix}:{order.OrderId}" }),
                context));
    }
}

public sealed record Unrelated(string Id);

/// <summary>Publishes a payload that is not the message's projection.</summary>
[IntropyComponent("wrong-payload-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:wrong-payload-gateway")]
public sealed class WrongPayloadGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, Unrelated>(route => route
                .Steps<OrderDeserializer, OrderValidator, UnrelatedTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(u => u.Id, _ => DateTimeOffset.UnixEpoch)));
}

/// <summary>Transforms to a payload the component's message does not project to.</summary>
public sealed class UnrelatedTransformer : TransformStep<SourceOrder, Unrelated, Context>
{
    public override Task<(TechnicalStepResult<Unrelated> Result, Context Context)> ExecuteAsync(
        SourceOrder input, Context context, CancellationToken ct) => throw new NotSupportedException();
}
