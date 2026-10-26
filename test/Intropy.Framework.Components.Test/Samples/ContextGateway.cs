using System.Text.Json;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Components;
using Intropy.Framework.Components.Gateway;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Components.Test.Samples;

/// <summary>A component's own context: what its steps share beyond the metadata.</summary>
public sealed record TenantContext(Dictionary<string, string> Metadata, bool IsRetry, string Tenant)
    : Context(Metadata, IsRetry);

/// <summary>Runs its pipeline on <see cref="TenantContext"/>. The factory starts from an empty
/// dictionary on purpose: the gateway's own metadata must land on the context anyway.</summary>
public sealed class TenantGateway : GatewayComponent
{
    protected override ComponentManifest Manifest { get; } = new()
    {
        ComponentName = "tenant-gateway", Organization = "Acme", Publishes = "order-placed",
        EventSource = "urn:Acme:tenant-gateway", Port = "order-gateway-endpoint", EndpointPath = "/orders",
    };

    protected override void Inputs(GatewayInputs inputs) => inputs
        .Accepts<OrderRequest>()
        .WithContext((_, isRetry) => new TenantContext([], isRetry, Tenant: "acme"))
        .Publishes<OrderPlaced>(pipeline => pipeline
            .Steps(steps => steps
                .Deserialize<TenantDeserializer>()
                .Validate<TenantValidator>()
                .Extract<TenantLookup>()
                .Transform<TenantTransformer>())
            .Idempotency(key: (order, context) => $"{context.Tenant}/{order.OrderId}", at: (order, _) => order.OrderDate)
            .OutputEvent(o => o.OrderId, o => o.OrderDate));
}

public sealed class TenantDeserializer : DeserializeStep<OrderRequest, TenantContext>
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public override Task<(BusinessStepResult<OrderRequest> Result, TenantContext Context)> ExecuteAsync(
        ReadOnlyMemory<byte> input, TenantContext context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, TenantContext)>(
            (new BusinessStepResult<OrderRequest>.Success(JsonSerializer.Deserialize<OrderRequest>(input.Span, Options)!),
                context));
}

public sealed class TenantValidator : ValidateStep<OrderRequest, TenantContext>
{
    public override Task<(BusinessStepResult<OrderRequest> Result, TenantContext Context)> ExecuteAsync(
        OrderRequest input, TenantContext context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, TenantContext)>(
            (new BusinessStepResult<OrderRequest>.Success(input), context));
}

/// <summary>Reads the component's context and the gateway's metadata on it.</summary>
public sealed class TenantLookup : ExtractStep<OrderRequest, TenantContext>
{
    public override Task<(BusinessStepResult<OrderRequest> Result, TenantContext Context)> ExecuteAsync(
        OrderRequest input, TenantContext context, CancellationToken ct) =>
        Task.FromResult<(BusinessStepResult<OrderRequest>, TenantContext)>(
            (new BusinessStepResult<OrderRequest>.Success(input with
            {
                OrderId = $"{input.OrderId}:{context.Tenant}:{context.Metadata[SourceContextKeys.RequestId]}",
            }), context));
}

public sealed class TenantTransformer : TransformStep<OrderRequest, OrderPlaced, TenantContext>
{
    public override Task<(TechnicalStepResult<OrderPlaced> Result, TenantContext Context)> ExecuteAsync(
        OrderRequest input, TenantContext context, CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<OrderPlaced>, TenantContext)>(
            (new TechnicalStepResult<OrderPlaced>.Success(new OrderPlaced(input.OrderId, input.OrderDate)), context));
}
