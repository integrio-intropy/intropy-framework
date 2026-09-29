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

    /// <summary>Whether the route runs whole batches (and so needs bulk delivery).</summary>
    bool IsBatch { get; }

    /// <summary>Builds the route's pipeline once, so a missing registration fails at startup.</summary>
    void Verify(IServiceProvider scope, string componentName);

    /// <summary>Runs <paramref name="messages"/> through the route: one at a time, each in its own
    /// scope, for a message route; as one batch in one scope for a batch route.</summary>
    /// <param name="messages">The route's messages, in delivery order.</param>
    /// <param name="scopes">Creates the DI scopes the pipelines are built in.</param>
    /// <param name="componentName">The component, for the pipeline name and errors.</param>
    /// <param name="interrupt">Cancelled when the host interrupts in-flight work; distinguishes an
    /// interruption from a failed or timed-out pipeline.</param>
    /// <param name="cancellationToken">Cancels the pipelines.</param>
    /// <returns>One outcome per message, in the order of <paramref name="messages"/>.</returns>
    Task<IReadOnlyList<PipelineOutcome>> ExecuteAsync(IReadOnlyList<IncomingMessage> messages,
        IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken);
}

internal sealed class LoaderRoute<TInput, TOutput, TCtx>(
    string? eventType,
    Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, LoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
    ContextFactory<TCtx> contextFactory) : ILoaderRoute where TCtx : Context
{
    public string? EventType => eventType;

    public string Name => eventType ?? "*";

    public bool IsBatch => false;

    public void Verify(IServiceProvider scope, string componentName) => Build(scope, componentName);

    public async Task<IReadOnlyList<PipelineOutcome>> ExecuteAsync(IReadOnlyList<IncomingMessage> messages,
        IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        var outcomes = new List<PipelineOutcome>(messages.Count);
        foreach (var message in messages)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var pipeline = Build(scope.ServiceProvider, componentName);
                var context = ContextCreation.Create(contextFactory, new Dictionary<string, string>(),
                    message.IsRetry, componentName, message.MessageId);
                var (result, _) = await pipeline.Execute(message.CloudEvent, context, detachTrace: false,
                    cancellationToken);
                outcomes.Add(PipelineOutcome.From(result, interrupt));
            }
            catch (Exception e)
            {
                outcomes.Add(PipelineOutcome.FromException(e, interrupt, cancellationToken));
            }
        }

        return outcomes;
    }

    private Loader<TInput, TOutput, TCtx> Build(IServiceProvider scope, string componentName) =>
        RouteComposition.Build(eventType, componentName, () =>
        {
            var builder = eventType is null
                ? LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process", scope)
                : LoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process.{eventType}", scope, eventType);
            return configurePipeline(builder, scope).Build();
        });
}

internal sealed class BatchLoaderRoute<TInput, TEnriched, TOutput, TCtx>(
    string? eventType,
    Func<BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>, IServiceProvider, BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>> configurePipeline,
    ContextFactory<TCtx> contextFactory) : ILoaderRoute where TCtx : Context
{
    public string? EventType => eventType;

    public string Name => eventType ?? "*";

    public bool IsBatch => true;

    public void Verify(IServiceProvider scope, string componentName) => Build(scope, componentName);

    public async Task<IReadOnlyList<PipelineOutcome>> ExecuteAsync(IReadOnlyList<IncomingMessage> messages,
        IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var pipeline = Build(scope.ServiceProvider, componentName);
            var entries = messages.Select(m => new BatchEntry<TCtx>(m.MessageId, m.CloudEvent,
                ContextCreation.Create(contextFactory, new Dictionary<string, string>(), m.IsRetry, componentName,
                    m.MessageId))).ToList();

            var results = await pipeline.ExecuteAsync(entries, cancellationToken);
            return [.. results.Select(r => r.Filtered
                ? new PipelineOutcome(MessageOutcome.Filtered)
                : PipelineOutcome.From(r.Result, interrupt))];
        }
        catch (Exception e)
        {
            // The batch as a whole failed (composition, or a step that threw past its own handling):
            // every entry is left for redelivery.
            var outcome = PipelineOutcome.FromException(e, interrupt, cancellationToken);
            return [.. messages.Select(_ => outcome)];
        }
    }

    private BatchLoader<TInput, TEnriched, TOutput, TCtx> Build(IServiceProvider scope, string componentName) =>
        RouteComposition.Build(eventType, componentName, () =>
        {
            var builder = eventType is null
                ? BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>.Create($"{componentName}.Process", scope)
                : BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>.Create($"{componentName}.Process.{eventType}",
                    scope, eventType);
            return configurePipeline(builder, scope).Build();
        });
}

internal static class RouteComposition
{
    /// <summary>Builds a route's pipeline, naming the component and route in a composition error.</summary>
    internal static T Build<T>(string? eventType, string componentName, Func<T> build)
    {
        try
        {
            return build();
        }
        catch (InvalidOperationException error)
        {
            var route = eventType is null ? "" : $" route '{eventType}'";
            throw new InvalidOperationException($"Loader '{componentName}'{route} composition failed: {error.Message}",
                error);
        }
    }
}
