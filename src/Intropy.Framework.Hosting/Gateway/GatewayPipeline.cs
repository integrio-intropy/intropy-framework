using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Extractor;

namespace Intropy.Framework.Hosting.Gateway;

/// <summary>
/// One endpoint's pipeline with its context type bound, so the endpoint handler needs none: the
/// endpoint's route and the factory that creates each request's context.
/// </summary>
internal abstract class GatewayPipeline
{
    /// <summary>Composes the pipeline once in <paramref name="scope"/>; throws what composition
    /// would throw on the first request.</summary>
    public abstract void Validate(IServiceProvider scope, string componentName, string port);

    /// <summary>Runs one request body through the pipeline, in the request's
    /// <paramref name="scope"/>, on a context created from the host-owned
    /// <paramref name="metadata"/>.</summary>
    public abstract Task<StepResult<CloudEvent>> RunAsync(IServiceProvider scope, string componentName, string port,
        ReadOnlyMemory<byte> body, Dictionary<string, string> metadata, string requestId, CancellationToken ct);
}

/// <summary>An endpoint's pipeline on <typeparamref name="TCtx"/>.</summary>
/// <typeparam name="TCtx">The pipeline's context type.</typeparam>
internal sealed class GatewayPipeline<TCtx>(ExtractorRoute<TCtx> route, ContextFactory<TCtx> contextFactory)
    : GatewayPipeline where TCtx : Context
{
    public override void Validate(IServiceProvider scope, string componentName, string port) =>
        _ = route.Resolve(scope, componentName, port);

    public override async Task<StepResult<CloudEvent>> RunAsync(IServiceProvider scope, string componentName,
        string port, ReadOnlyMemory<byte> body, Dictionary<string, string> metadata, string requestId,
        CancellationToken ct)
    {
        // The host-owned metadata ends up on the context whatever the component's factory does.
        var context = ContextCreation.Create(contextFactory, metadata, isRetry: false, componentName, requestId);
        var run = route.Resolve(scope, componentName, port);
        return await run(body, context, ct);
    }
}
