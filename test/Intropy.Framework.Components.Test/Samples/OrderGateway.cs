using System.Text.Json;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Components.Test.Samples;

public sealed record SourceOrder(string OrderId, DateTimeOffset OrderDate, int Lines);

public sealed record OrderPlaced(string OrderId, DateTimeOffset OrderDate);

/// <summary>The component under test: the attributed endpoint, named in full, and what it accepts.</summary>
[IntropyComponent("order-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:order-gateway")]
public sealed class OrderGateway : GatewayComponent
{
    public const string Endpoint = "order-gateway-endpoint";

    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(Endpoint, "/orders", endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency(key: (order, _) => order.OrderId, at: (order, _) => order.OrderDate)
                .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate)));
}

/// <summary>What an attributed gateway declares: the endpoint the component's identity names, and
/// what it accepts.</summary>
[IntropyComponent("order-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
    Payload = typeof(OrderPlaced), Port = "order-gateway-endpoint", EndpointPath = "/orders",
    EventSource = "urn:Acme:order-endpoint-gateway")]
public sealed class RecordedEndpointGateway : GatewayComponent
{
    protected override void Endpoints(GatewayEndpoints endpoints) =>
        endpoints.Endpoint(endpoint => endpoint
            .Accepts<SourceOrder, OrderPlaced>(route => route
                .Steps<OrderDeserializer, OrderValidator, OrderTransformer>()
                .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
                .OutputEvent(o => o.OrderId, o => o.OrderDate)));
}

public sealed class OrderDeserializer : DeserializeStep<SourceOrder, Context>
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<SourceOrder> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var order = JsonSerializer.Deserialize<SourceOrder>(input.Span, Options)!;
        context.Metadata[BusinessIncidentContextKeys.BusinessId] = order.OrderId;
        return Task.FromResult<(BusinessStepResult<SourceOrder>, Context)>(
            (new BusinessStepResult<SourceOrder>.Success(order), context));
    }
}

public sealed class OrderValidator : ValidateStep<SourceOrder, Context>
{
    public override Task<(BusinessStepResult<SourceOrder> Result, Context Context)> ExecuteAsync(
        SourceOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<SourceOrder>, Context)>(input.Lines > 0
            ? (new BusinessStepResult<SourceOrder>.Success(input), context)
            : (new BusinessStepResult<SourceOrder>.Failure(new BusinessIncidentData { Description = "Order has no lines" }), context));
}

public sealed class OrderTransformer : TransformStep<SourceOrder, OrderPlaced, Context>
{
    public override Task<(TechnicalStepResult<OrderPlaced> Result, Context Context)> ExecuteAsync(
        SourceOrder input, Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderPlaced>, Context)>(
            (new TechnicalStepResult<OrderPlaced>.Success(new OrderPlaced(input.OrderId, input.OrderDate)), context));
}
