using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Components.Test.Samples;

/// <summary>An EDI line: another source system's shape of the same order.</summary>
public sealed record EdiOrder(string Reference, DateTimeOffset Issued);

/// <summary>Two endpoints, each with its own input type, publishing the one message: the recorded
/// endpoint takes JSON orders, a second takes EDI lines.</summary>
[IntropyComponent("two-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = TwoEndpointGateway.Json, EndpointPath = "/orders",
    EventSource = "urn:Acme:two-endpoint-gateway")]
public sealed class TwoEndpointGateway : GatewayComponent
{
    public const string Json = "order-gateway-endpoint";
    public const string Edi = "order-gateway-edi";

    protected override void Endpoints(GatewayEndpoints endpoints) => endpoints
        .Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)))
        .Endpoint(Edi, "/orders/edi", endpoint => endpoint
            .Accepts<EdiOrder, OrderPlaced>(route => route
                .Steps<EdiDeserializer, EdiValidator, EdiTransformer>()
                .Idempotency((o, _) => o.Reference, (o, _) => o.Issued)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)));
}

/// <summary>Parses "reference;issued".</summary>
public sealed class EdiDeserializer : DeserializeStep<EdiOrder, Context>
{
    public override Task<(BusinessStepResult<EdiOrder> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var parts = System.Text.Encoding.UTF8.GetString(input.Span).Split(';');
        return Task.FromResult<(BusinessStepResult<EdiOrder>, Context)>(parts.Length == 2
            ? (new BusinessStepResult<EdiOrder>.Success(new EdiOrder(parts[0], DateTimeOffset.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture))), context)
            : (new BusinessStepResult<EdiOrder>.Failure(new BusinessIncidentData { Description = "Not an EDI line" }), context));
    }
}

public sealed class EdiValidator : ValidateStep<EdiOrder, Context>
{
    public override Task<(BusinessStepResult<EdiOrder> Result, Context Context)> ExecuteAsync(
        EdiOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<EdiOrder>, Context)>((new BusinessStepResult<EdiOrder>.Success(input), context));
}

public sealed class EdiTransformer : TransformStep<EdiOrder, OrderPlaced, Context>
{
    public override Task<(TechnicalStepResult<OrderPlaced> Result, Context Context)> ExecuteAsync(
        EdiOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderPlaced>, Context)>(
            (new TechnicalStepResult<OrderPlaced>.Success(new OrderPlaced(input.Reference, input.Issued)), context));
}

/// <summary>Two endpoints accepting the same input, each with its own steps: the framework routes
/// them by declaration, not by a class found once in the assembly.</summary>
[IntropyComponent("twin-step-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = TwinStepGateway.Main, EndpointPath = "/orders",
    EventSource = "urn:Acme:twin-step-gateway")]
public sealed class TwinStepGateway : GatewayComponent
{
    public const string Main = "order-gateway-endpoint";
    public const string Twin = "order-gateway-twin";

    protected override void Endpoints(GatewayEndpoints endpoints) => endpoints
        .Endpoint(Main, "/orders", endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)))
        .Endpoint(Twin, "/orders/twin", endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<AltOrderDeserializer, AltOrderValidator, AltOrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => $"twin:{o.OrderId}", o => o.OrderDate)));
}

/// <summary>Deserializes like <see cref="OrderDeserializer"/> without naming the item.</summary>
public sealed class AltOrderDeserializer : DeserializeStep<SourceOrder, Context>
{
    private static readonly System.Text.Json.JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<SourceOrder> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<SourceOrder>, Context)>(
            (new BusinessStepResult<SourceOrder>.Success(
                System.Text.Json.JsonSerializer.Deserialize<SourceOrder>(input.Span, Options)!), context));
}

/// <summary>Validates like <see cref="OrderValidator"/> with another class.</summary>
public sealed class AltOrderValidator : ValidateStep<SourceOrder, Context>
{
    public override Task<(BusinessStepResult<SourceOrder> Result, Context Context)> ExecuteAsync(
        SourceOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<SourceOrder>, Context)>(input.Lines > 0
            ? (new BusinessStepResult<SourceOrder>.Success(input), context)
            : (new BusinessStepResult<SourceOrder>.Failure(
                new BusinessIncidentData { Description = "Order has no lines" }), context));
}

/// <summary>Transforms like <see cref="OrderTransformer"/> with another class.</summary>
public sealed class AltOrderTransformer : TransformStep<SourceOrder, OrderPlaced, Context>
{
    public override Task<(TechnicalStepResult<OrderPlaced> Result, Context Context)> ExecuteAsync(
        SourceOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderPlaced>, Context)>(
            (new TechnicalStepResult<OrderPlaced>.Success(new OrderPlaced(input.OrderId, input.OrderDate)), context));
}
