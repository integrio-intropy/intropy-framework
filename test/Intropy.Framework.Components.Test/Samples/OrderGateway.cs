using System.Text.Json;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Components.Test.Samples;

public sealed record OrderRequest(string OrderId, DateTimeOffset OrderDate, int Lines);

public sealed record OrderPlaced(string OrderId, DateTimeOffset OrderDate);

/// <summary>The component under test: the manifest's endpoint, named in full, and what it accepts.</summary>
public sealed class OrderGateway : GatewayComponent
{
    public const string Endpoint = "order-gateway-endpoint";

    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "order-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:order-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .On(Endpoint, "/orders")
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency(key: (order, _) => order.OrderId, at: (order, _) => order.OrderDate)
            .OutputEvent(subject: order => order.OrderId, at: order => order.OrderDate));
}

/// <summary>What a gateway declares: the endpoint the component's identity names, and
/// what it accepts.</summary>
public sealed class RecordedEndpointGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "order-endpoint-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:order-endpoint-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<OrderDeserializer>()
                .Validate<OrderValidator>()
                .Transform<OrderTransformer>())
            .Idempotency((o, _) => o.OrderId, (o, _) => o.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

public sealed class OrderDeserializer : DeserializeStep<OrderRequest, Context>
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<OrderRequest> Result, Context Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, Context context, CancellationToken ct)
    {
        var order = JsonSerializer.Deserialize<OrderRequest>(input.Span, Options)!;
        context.Metadata[BusinessIncidentContextKeys.BusinessId] = order.OrderId;
        return Task.FromResult<(BusinessStepResult<OrderRequest>, Context)>(
            (new BusinessStepResult<OrderRequest>.Success(order), context));
    }
}

public sealed class OrderValidator : ValidateStep<OrderRequest, Context>
{
    public override Task<(BusinessStepResult<OrderRequest> Result, Context Context)> ExecuteAsync(
        OrderRequest input, Context context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, Context)>(input.Lines > 0
            ? (new BusinessStepResult<OrderRequest>.Success(input), context)
            : (new BusinessStepResult<OrderRequest>.Failure(new BusinessIncidentData { Description = "Order has no lines" }), context));
}

public sealed class OrderTransformer : TransformStep<OrderRequest, OrderPlaced, Context>
{
    public override Task<(TechnicalStepResult<OrderPlaced> Result, Context Context)> ExecuteAsync(
        OrderRequest input, Context context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderPlaced>, Context)>(
            (new TechnicalStepResult<OrderPlaced>.Success(new OrderPlaced(input.OrderId, input.OrderDate)), context));
}
