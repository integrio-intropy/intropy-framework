using Intropy.Framework.Blocks.Loader;
using Microsoft.Extensions.Configuration;
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

    /// <summary>Checks the definition and builds the validated subscription options: configuration
    /// and the <see cref="Configure"/> tweaks bind onto the options first, and the definition's own
    /// <see cref="PubSubName"/>/<see cref="TopicName"/> are applied on top when the code set them,
    /// so the final options always carry what this definition meant.</summary>
    /// <returns>The subscription options to register.</returns>
    /// <exception cref="InvalidOperationException"><see cref="Pipeline"/> is missing, or neither
    /// the definition, its <see cref="Configure"/>, nor a bound configuration section provides the
    /// pub/sub or topic.</exception>
    internal LoaderOptions Validate()
    {
        if (Pipeline is null)
            throw new InvalidOperationException(
                "Loader composition failed: Pipeline — the definition has no pipeline configurator; set Pipeline.");

        var options = new LoaderOptions();
        Configure?.Invoke(options);
        // The definition's required names are authoritative when the code set them: configuration
        // binds first, Configure runs second, and a set definition member beats both. An unset
        // member (the configuration-bound shape) defers to the options the merged Configure has
        // already bound — but the value this definition registered is always validated.
        if (NonEmpty(PubSubName))
            options.PubSubName = PubSubName;
        if (NonEmpty(TopicName))
            options.TopicName = TopicName;
        if (string.IsNullOrWhiteSpace(options.PubSubName))
            throw new InvalidOperationException(
                "Loader composition failed: PubSubName — the loader needs the Dapr pub/sub component to subscribe through; set PubSubName.");
        if (string.IsNullOrWhiteSpace(options.TopicName))
            throw new InvalidOperationException(
                "Loader composition failed: TopicName — the loader needs the topic to consume; set TopicName.");
        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort, nameof(LoaderOptions), "definition");
        return options;
    }

    /// <summary>Builds the definition the configuration-based registration registers: the section's
    /// key values bind onto the subscription options before the definition's own
    /// <see cref="Configure"/> runs, so code wins over configuration, and a definition member the
    /// factory sets stays authoritative over both.</summary>
    /// <param name="configuration">The section holding the loader's values: <c>PubSubName</c>,
    /// <c>TopicName</c>, <c>Unrouted</c>, <c>MaxMessageProcessingTime</c>,
    /// <c>ShutdownGracePeriod</c> and <c>CallbackPort</c>. Each value only sets what code does not;
    /// a malformed value fails at registration, with the member it would configure.</param>
    /// <param name="factory">Builds the definition the code owns — the pipeline, the context
    /// factory, and any value configuration must not overrule. Leave a member unset (null) to defer
    /// it to the configuration — including a required pub/sub or topic the section provides.</param>
    /// <returns>A definition equivalent to the factory's, with the configuration supplying the
    /// values the definition leaves unset.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> or
    /// <paramref name="factory"/> is null.</exception>
    internal static LoaderDefinition<TInput, TOutput, TCtx> BoundTo(IConfiguration configuration,
        Func<LoaderDefinition<TInput, TOutput, TCtx>> factory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(factory);
        var source = factory();
        return new LoaderDefinition<TInput, TOutput, TCtx>
        {
            // Unset members pass through: Validate() defers them to the options the merged
            // Configure has already bound the section's values onto.
            PubSubName = source.PubSubName,
            TopicName = source.TopicName,
            Configure = options =>
            {
                LoaderServiceCollectionExtensions.ApplyConfiguration(options, configuration);
                source.Configure?.Invoke(options);
            },
            Pipeline = source.Pipeline,
            ContextFactory = source.ContextFactory,
        };
    }

    private static bool NonEmpty(string? value) => !string.IsNullOrWhiteSpace(value);
}
