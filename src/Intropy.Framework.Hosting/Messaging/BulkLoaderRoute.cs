using System.Diagnostics;
using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>One route of a bulk loader: the messages of one CloudEvent type, or of every type (a
/// null event type: the single route of a bulk loader without routes), through a bulk loader
/// pipeline built once per batch, in the batch's own scope.</summary>
/// <remarks>
/// The scope is the batch's, not each message's — the point of a bulk loader is one external call
/// for the whole batch, so the steps that make it are resolved once. Each message still gets its
/// own context and its own outcome.
/// </remarks>
internal sealed class BulkLoaderRoute<TInput, TOutput, TCtx>(
    string? eventType,
    Func<BulkLoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider, BulkLoaderBuilder<TInput, TOutput, TCtx>> configurePipeline,
    ContextFactory<TCtx> contextFactory) : IBulkMessageRoute where TCtx : Context
{
    public string Name => eventType ?? "*";

    public bool IsCatchAll => eventType is null;

    public bool Matches(IncomingMessage message) =>
        eventType is null || string.Equals(message.Type, eventType, StringComparison.Ordinal);

    public void Verify(IServiceProvider scope, string componentName) => Build(scope, componentName);

    public async Task<IReadOnlyList<PipelineOutcome>> ExecuteBatchAsync(IReadOnlyList<IncomingMessage> messages,
        IReadOnlyList<Activity?> spans, IServiceScopeFactory scopes, string componentName, CancellationToken interrupt,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
            return [];

        var outcomes = new PipelineOutcome?[messages.Count];
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var pipeline = Build(scope.ServiceProvider, componentName);

            // Each message's attributes go on its own context, and its payload is resolved the same
            // way a single-message route resolves one: a claim-checked message does not carry the
            // payload it names. Resolution happens after routing so an unrouted batch never pays a
            // fetch. A message that cannot be prepared — a claim pointer this framework did not
            // mint, a claim store it cannot read — fails on its own and stays out of the batch, as
            // it would on a single-message route. Failing the whole batch with it would send every
            // other message back for redelivery, and the poison message would come back in the next
            // batch to do it again.
            var batch = new List<(ReadOnlyMemory<byte> Input, TCtx Context)>(messages.Count);
            var positions = new List<int>(messages.Count);
            for (var i = 0; i < messages.Count; i++)
            {
                var message = messages[i];

                // What preparing a message calls, a claim store say, is traced under the message.
                Activity.Current = spans[i];
                try
                {
                    var context = ContextCreation.Create(contextFactory, LoaderRoute<TInput, TOutput, TCtx>
                        .ProjectAttributes(message), message.IsRetry, componentName, message.MessageId);
                    var payload = await ClaimResolution.ResolveAsync(message, scope.ServiceProvider,
                        cancellationToken);
                    batch.Add((payload, context));
                    positions.Add(i);
                }
                catch (Exception e) when (!cancellationToken.IsCancellationRequested)
                {
                    outcomes[i] = PipelineOutcome.FromException(e, interrupt, cancellationToken);
                }
                finally
                {
                    Activity.Current = null;
                }
            }

            if (batch.Count > 0)
            {
                // A message whose own span is not recorded is still traced under the context it
                // carries, so its pipeline stays in its producer's trace.
                var results = await pipeline.Execute(batch,
                    [.. positions.Select(i => spans[i]?.Context ?? MessageActivity.PropagatedContext(messages[i]))],
                    cancellationToken);
                for (var j = 0; j < results.Count; j++)
                    outcomes[positions[j]] = PipelineOutcome.From(results[j].Result, interrupt);
            }
        }
        catch (Exception e)
        {
            // What failed here failed the batch as a whole — building the pipeline, the run being
            // cancelled, or the bulk send throwing past its own mapping. Every message that had no
            // verdict of its own yet is left for redelivery with that cause.
            var outcome = PipelineOutcome.FromException(e, interrupt, cancellationToken);
            for (var i = 0; i < outcomes.Length; i++)
                outcomes[i] ??= outcome;
        }

        // The pipeline answers one result per message it was given; a message it left without one is
        // left for redelivery rather than reported as handled.
        return [.. outcomes.Select(o => o ?? PipelineOutcome.Failed("missing_outcome",
            "The bulk pipeline returned no result for this message"))];
    }

    private BulkLoader<TInput, TOutput, TCtx> Build(IServiceProvider scope, string componentName) =>
        RouteComposition.Build("Bulk loader", eventType, componentName, () =>
        {
            var builder = eventType is null
                ? BulkLoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process", scope)
                : BulkLoaderBuilder<TInput, TOutput, TCtx>.Create($"{componentName}.Process.{eventType}", scope,
                    eventType);
            return configurePipeline(builder, scope).Build();
        });
}
