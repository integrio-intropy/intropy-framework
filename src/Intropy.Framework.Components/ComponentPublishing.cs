using Microsoft.Extensions.DependencyInjection.Extensions;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// What every component that publishes shares, whatever brings its items in: the one publisher of
/// its one message, typed and sourced from the manifest.
/// </summary>
internal static class ComponentPublishing
{
    /// <summary>Registers the publisher. From the manifest: the event type
    /// (<see cref="ComponentManifest.Publishes"/>) and the event source
    /// (<see cref="ComponentManifest.EventSource"/>). From the deployment: the pub/sub and the topic
    /// (<c>INTROPY_PUBSUB_NAME</c>, <c>INTROPY_TOPIC_NAME</c>), resolved when a pipeline is first
    /// composed, at startup.</summary>
    internal static void AddPublisher(IServiceCollection services, ComponentManifest manifest)
    {
        var eventSource = new Uri(manifest.EventSource);
        // The publisher is a service like the other externals, so a test swaps the
        // SendStep<Context> registration for a fake exactly as it swaps the clients.
        services.AddSingleton<SendStep<Context>>(provider =>
        {
            var options = provider.GetRequiredService<FrameworkOptions>();
            return new DaprTopicPublisher<Context>(
                provider.GetRequiredService<DaprClient>(),
                options.PubSubName ?? throw new InvalidOperationException(
                    "INTROPY_PUBSUB_NAME is not set, so the system's pub/sub is unknown: run the " +
                    "component under its system host, or set FrameworkOptions.PubSubName."),
                options.TopicName ?? throw new InvalidOperationException(
                    "INTROPY_TOPIC_NAME is not set, so the topic is unknown: run the component " +
                    "under its system host, or set FrameworkOptions.TopicName."),
                eventSource,
                manifest.Publishes);
        });
    }

    /// <summary>Makes the one publisher available to a pipeline on the component's own context type:
    /// a <see cref="SendStep{TCtx}"/> of <typeparamref name="TCtx"/> that sends through whatever
    /// <see cref="SendStep{TCtx}"/> of <see cref="Context"/> is registered — so a test that swaps the
    /// publisher swaps it for every pipeline. A sender the component registers itself stays its
    /// own.</summary>
    internal static void AddPublisherOn<TCtx>(IServiceCollection services) where TCtx : Context
    {
        if (typeof(TCtx) == typeof(Context))
            return;
        services.TryAddSingleton<SendStep<TCtx>>(provider =>
            new ContextSendStep<TCtx>(provider.GetRequiredService<SendStep<Context>>()));
    }

    /// <summary>The publisher on <typeparamref name="TCtx"/>: sends through the publisher on
    /// <see cref="Context"/>, with its step name and envelope defaults, so the claim check measures
    /// the same envelope.</summary>
    private sealed class ContextSendStep<TCtx>(SendStep<Context> publisher) : SendStep<TCtx> where TCtx : Context
    {
        public override string StepName => publisher.StepName;

        protected internal override (Uri Source, string Type)? EnvelopeDefaults => publisher.EnvelopeDefaults;

        public override async Task<(TechnicalStepResult<CloudEvent> Result, TCtx Context)> ExecuteAsync(
            CloudEvent input, TCtx context, CancellationToken ct)
        {
            var (result, next) = await publisher.ExecuteAsync(input, context, ct);
            // A publisher that returns a context of its own keeps it when it is still the pipeline's type.
            return (result, next as TCtx ?? context);
        }
    }
}
