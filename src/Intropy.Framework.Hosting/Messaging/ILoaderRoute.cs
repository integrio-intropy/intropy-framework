using CloudNative.CloudEvents;
using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>One route of a loader, with its generic types closed over.</summary>
internal interface ILoaderRoute
{
    /// <summary>The CloudEvent type the route handles; null for the single route of a loader
    /// without routes, which handles every type.</summary>
    string? EventType { get; }

    /// <summary>The route's name in logs, spans and metrics.</summary>
    string Name { get; }

    /// <summary>Builds the route's pipeline once, so a missing registration fails at startup.</summary>
    void Verify(IServiceProvider scope, string componentName);

    /// <summary>Runs <paramref name="message"/> through the route, in its own DI scope.</summary>
    /// <param name="message">The consumed message.</param>
    /// <param name="cloudEvent">The message as the CloudEvent the route's pipeline consumes.</param>
    /// <param name="scopes">Creates the DI scope the pipeline is built in.</param>
    /// <param name="componentName">The component, for the pipeline name and errors.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work; distinguishes an
    /// interruption from a failed or timed-out pipeline.</param>
    /// <param name="cancellationToken">Cancels the pipeline.</param>
    /// <returns>The message's outcome.</returns>
    Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, CloudEvent cloudEvent, IServiceScopeFactory scopes,
        string componentName, CancellationToken interrupt, CancellationToken cancellationToken);
}

internal sealed class LoaderRoute<TInput, TOutput, TCtx>(
    string? eventType,
    Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
    ContextFactory<TCtx> contextFactory) : ILoaderRoute where TCtx : Context
{
    public string? EventType => eventType;

    public string Name => eventType ?? "*";

    public void Verify(IServiceProvider scope, string componentName) => Build(scope, componentName);

    public async Task<PipelineOutcome> ExecuteAsync(IncomingMessage message, CloudEvent cloudEvent,
        IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var pipeline = Build(scope.ServiceProvider, componentName);
            var context = ContextCreation.Create(contextFactory, new Dictionary<string, string>(),
                message.IsRetry, componentName, message.MessageId);
            var (result, _) = await pipeline.Execute(cloudEvent, context, detachTrace: false, cancellationToken);
            return PipelineOutcome.From(result, interrupt);
        }
        catch (Exception e)
        {
            return PipelineOutcome.FromException(e, interrupt, cancellationToken);
        }
    }

    private Loader<TInput, TOutput, TCtx> Build(IServiceProvider scope, string componentName) =>
        RouteComposition.Build("Loader", eventType, componentName, () =>
        {
            var builder = eventType is null
                ? LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process", scope)
                : LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process.{eventType}", scope, eventType);
            return configurePipeline(builder, scope).Build();
        });
}

internal static class RouteComposition
{
    /// <summary>Builds a route's pipeline, naming the block kind, the component and the route in a
    /// composition error.</summary>
    internal static T Build<T>(string blockKind, string? eventType, string componentName, Func<T> build)
    {
        try
        {
            return build();
        }
        catch (InvalidOperationException error)
        {
            var route = eventType is null ? "" : $" route '{eventType}'";
            throw new InvalidOperationException($"{blockKind} '{componentName}'{route} composition failed: {error.Message}",
                error);
        }
    }
}
