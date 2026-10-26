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
public sealed class CustomOrderGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "custom-order-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:custom-order-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => $"custom:{o.OrderId}", o => o.OrderDate)
            .Incidents(new IncidentRule(_ => "custom-id", _ => "custom-subject")));

    protected override void ConfigureOptions(GatewayOptions options) => options.MaxRequestBodyBytes = 256;

    protected override IResult Respond(GatewayOutcome outcome) => outcome is GatewayOutcome.Published
        ? Results.StatusCode(StatusCodes.Status201Created)
        : GatewayResponses.Default(outcome);
}

/// <summary>Exposes the manifest's endpoint at another path than the manifest's.</summary>
public sealed class MovedPathGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "moved-path-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:moved-path-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) =>
        PublishesOrders(inputs.On(OrderGateway.Endpoint, "/elsewhere").Accepts<OrderRequest>());

    internal static GatewayInputs PublishesOrders(GatewayInput<OrderRequest> input) =>
        input.Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

/// <summary>Renames the manifest's endpoint.</summary>
public sealed class RenamedEndpointGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "renamed-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:renamed-endpoint-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) =>
        MovedPathGateway.PublishesOrders(inputs.On("orders-in", "/orders").Accepts<OrderRequest>());
}

/// <summary>Names an endpoint that accepts nothing.</summary>
public sealed class AcceptingNothingGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "accepting-nothing-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:accepting-nothing-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) =>
        inputs.On("order-gateway-endpoint", "/orders");
}

/// <summary>Has its endpoint accept two kinds of data.</summary>
public sealed class AcceptingTwiceGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "accepting-twice-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:accepting-twice-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs)
    {
        MovedPathGateway.PublishesOrders(inputs.Accepts<OrderRequest>());
        inputs.Accepts<EdiOrder>()
            .Publishes<OrderPlaced>(pipeline => pipeline
                .Steps(steps => steps
                    .Deserialize<EdiDeserializer>()
                    .Validate<EdiValidator>()
                    .Transform<EdiTransformer>())
                .Idempotency((o, _) => o.Reference, (o, _) => o.Issued)
                .OutputEvent(o => o.OrderId, o => o.OrderDate));
    }
}

/// <summary>Accepts orders without an idempotency rule.</summary>
public sealed class RulelessGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "ruleless-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:ruleless-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

public sealed record StepLessInput(string Id);

/// <summary>Declares a pipeline without steps.</summary>
public sealed class StepLessGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "stepless-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:stepless-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<StepLessInput>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Idempotency((i, _) => i.Id, (_, _) => DateTimeOffset.UnixEpoch)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

/// <summary>Names a deserializer that takes a service in its constructor, registered in
/// <c>ConfigureServices</c>.</summary>
public sealed class AuthorStepsGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "author-steps-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:author-steps-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<AuthorDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));

    protected override void ConfigureServices(IServiceCollection services, IConfiguration configuration) =>
        services.AddSingleton(new Greeting("author"));
}

/// <summary>A service only the author's deserializer takes, to pin constructor injection.</summary>
/// <param name="Prefix">What the author's deserializer prefixes the order id with.</param>
public sealed record Greeting(string Prefix);

/// <summary>The author's deserializer: prefixes the order id, so the published event names whose
/// deserializer ran.</summary>
public sealed class AuthorDeserializer(Greeting greeting) : DeserializeStep<OrderRequest, Context>
{
    private static readonly System.Text.Json.JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<OrderRequest> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var order = System.Text.Json.JsonSerializer.Deserialize<OrderRequest>(input.Span, Options)!;
        return Task.FromResult<(BusinessStepResult<OrderRequest>, Context)>(
            (new BusinessStepResult<OrderRequest>.Success(order with { OrderId = $"{greeting.Prefix}:{order.OrderId}" }),
                context));
    }
}

/// <summary>Names its endpoint's port but no path: the endpoint is at <c>/&lt;publishes&gt;</c>.</summary>
public sealed class DefaultPathGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "default-path-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:default-path-gateway", Port = "order-gateway-endpoint",
    };

    protected override void Inputs(GatewayInputs inputs) =>
        MovedPathGateway.PublishesOrders(inputs.Accepts<OrderRequest>());
}

/// <summary>Declares the manifest's endpoint, but its manifest names no port.</summary>
public sealed class PortlessGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "portless-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:portless-gateway",
    };

    protected override void Inputs(GatewayInputs inputs) =>
        MovedPathGateway.PublishesOrders(inputs.Accepts<OrderRequest>());
}

/// <summary>Accepts orders but never says what it publishes.</summary>
public sealed class PublishingNothingGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "publishing-nothing-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:publishing-nothing-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs.Accepts<OrderRequest>();
}

/// <summary>Another payload than <see cref="OrderPlaced"/>, to pin that a gateway publishes one.</summary>
public sealed record OrderReceived(string Reference);

public sealed class OrderReceivedTransformer : TransformStep<EdiOrder, OrderReceived, Context>
{
    public override Task<(TechnicalStepResult<OrderReceived> Result, Context Context)> ExecuteAsync(
        EdiOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderReceived>, Context)>(
            (new TechnicalStepResult<OrderReceived>.Success(new OrderReceived(input.Reference)), context));
}

/// <summary>Has its two endpoints publish different types.</summary>
public sealed class MixedPayloadGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "mixed-payload-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:mixed-payload-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs)
    {
        MovedPathGateway.PublishesOrders(inputs.Accepts<OrderRequest>());
        inputs.On("order-gateway-edi", "/orders/edi")
            .Accepts<EdiOrder>()
            .Publishes<OrderReceived>(pipeline => pipeline
                .Steps(steps => steps
                    .Deserialize<EdiDeserializer>()
                    .Validate<EdiValidator>()
                    .Transform<OrderReceivedTransformer>())
                .Idempotency((o, _) => o.Reference, (o, _) => o.Issued)
                .OutputEvent(o => o.Reference, _ => DateTimeOffset.UnixEpoch));
    }
}

/// <summary>Publishes every order it accepts, duplicates included: no idempotency check.</summary>
public sealed class NoIdempotencyGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "no-idempotency-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:no-idempotency-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .NoIdempotency()
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

/// <summary>Declares an idempotency rule and no idempotency on the same pipeline.</summary>
public sealed class BothIdempotencyGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "both-idempotency-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:both-idempotency-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .NoIdempotency()
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

/// <summary>Enriches the order id with "first", to pin the extract steps' order.</summary>
public sealed class FirstLookup : ExtractStep<OrderRequest, Context>
{
    public override Task<(BusinessStepResult<OrderRequest> Result, Context Context)> ExecuteAsync(
        OrderRequest input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, Context)>(
            (new BusinessStepResult<OrderRequest>.Success(input with { OrderId = $"{input.OrderId}:first" }), context));
}

/// <summary>Enriches the order id with the greeting it takes in its constructor.</summary>
public sealed class GreetingLookup(Greeting greeting) : ExtractStep<OrderRequest, Context>
{
    public override Task<(BusinessStepResult<OrderRequest> Result, Context Context)> ExecuteAsync(
        OrderRequest input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, Context)>(
            (new BusinessStepResult<OrderRequest>.Success(input with { OrderId = $"{input.OrderId}:{greeting.Prefix}" }),
                context));
}

/// <summary>Enriches each order with two extract steps, the second taking a service.</summary>
public sealed class EnrichingGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "enriching-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:enriching-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Extract<FirstLookup>()
                .Extract<GreetingLookup>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));

    protected override void ConfigureServices(IServiceCollection services, IConfiguration configuration) =>
        services.AddSingleton(new Greeting("second"));
}
