using Intropy.Framework.Blocks.Loader;
using Microsoft.Extensions.DependencyInjection;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// Describes a loader component for
/// <see cref="LoaderServiceCollectionExtensions.AddLoader{TInput,TOutput,TCtx}(IServiceCollection, LoaderDefinition{TInput,TOutput,TCtx})"/>:
/// one pipeline for every message on the topic, the pub/sub and topic it consumes, and the
/// subscription settings around them. The definition is validated when it is registered, so a
/// misconfigured loader fails at startup with the member that is wrong and how to fix it.
/// </summary>
/// <remarks>
/// For a loader that routes each CloudEvent type to its own pipeline, use the routed overload
/// (<c>AddLoader(configure, configureRoutes)</c>) with <see cref="LoaderRoutes.On{TInput,TOutput,TCtx}"/>
/// and <see cref="LoaderRoutes.OnAny{TInput,TOutput,TCtx}"/> instead.
/// <para>Build the definition with an object initializer:</para>
/// <code>
/// services.AddLoader(new LoaderDefinition&lt;OrderCreated, OrderCreated, Context&gt;
/// {
///     PubSubName = "pubsub",
///     TopicName = "orders",
///     Pipeline = (builder, services) =&gt; builder
///         .WithDeserializer&lt;OrderDeserializer&gt;()
///         .WithIdempotency(),
/// });
/// </code>
/// </remarks>
/// <typeparam name="TInput">The deserialized input.</typeparam>
/// <typeparam name="TOutput">What the loader sends to the external system.</typeparam>
/// <typeparam name="TCtx">The pipeline context.</typeparam>
public sealed class LoaderDefinition<TInput, TOutput, TCtx> where TCtx : Context
{
    /// <summary>The Dapr pub/sub component to subscribe through. Required.</summary>
    public required string PubSubName { get; init; }

    /// <summary>The topic the loader consumes. Required.</summary>
    public required string TopicName { get; init; }

    /// <summary>The rest of the subscription: unrouted policy, timeouts, callback port. The
    /// definition's <see cref="PubSubName"/> and <see cref="TopicName"/> are authoritative —
    /// set through them, not here.</summary>
    public Action<LoaderOptions>? Configure { get; init; }

    /// <summary>Configures the pipeline builder, given the message's scope. This is where the
    /// generic step overloads (<c>WithDeserializer&lt;TStep&gt;()</c> and friends) save the
    /// cast-and-resolve boilerplate.</summary>
    public required Func<LoaderBuilder<TInput, TOutput, TCtx>, IServiceProvider,
        LoaderBuilder<TInput, TOutput, TCtx>> Pipeline { get; init; }

    /// <summary>Creates the context for each message. Required when <typeparamref name="TCtx"/>
    /// derives from <see cref="Context"/>; when <typeparamref name="TCtx"/> is the base
    /// <see cref="Context"/>, leave null and the plain record is created.</summary>
    public ContextFactory<TCtx>? ContextFactory { get; init; }

    /// <summary>Checks the definition and builds the validated subscription options: the
    /// <see cref="Configure"/> tweaks with the definition's required
    /// <see cref="PubSubName"/>/<see cref="TopicName"/> applied last, so the final options are
    /// always the ones this definition validated.</summary>
    /// <returns>The subscription options to register.</returns>
    /// <exception cref="InvalidOperationException"><see cref="Pipeline"/>, <see cref="PubSubName"/>
    /// or <see cref="TopicName"/> is missing.</exception>
    internal LoaderOptions Validate()
    {
        if (Pipeline is null)
            throw new InvalidOperationException(
                "Loader composition failed: Pipeline — the definition has no pipeline configurator; set Pipeline.");
        if (string.IsNullOrWhiteSpace(PubSubName))
            throw new InvalidOperationException(
                "Loader composition failed: PubSubName — the loader needs the Dapr pub/sub component to subscribe through; set PubSubName.");
        if (string.IsNullOrWhiteSpace(TopicName))
            throw new InvalidOperationException(
                "Loader composition failed: TopicName — the loader needs the topic to consume; set TopicName.");

        var options = new LoaderOptions();
        Configure?.Invoke(options);
        options.PubSubName = PubSubName;
        options.TopicName = TopicName;
        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort, nameof(LoaderOptions), "definition");
        return options;
    }
}
