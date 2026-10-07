using Intropy.Framework.Blocks.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.FileSweeps;
using Intropy.Framework.Hosting.Jobs;

namespace Intropy.Framework.Hosting.TransactionalIntegration;

/// <summary>
/// Describes a Transactional Integration for
/// <see cref="TransactionalIntegrationServiceCollectionExtensions.AddTransactionalIntegration{TCtx}(IServiceCollection, TransactionalIntegrationDefinition{TCtx})"/>:
/// the receive and send side settings, the context factory and the runner settings in one object.
/// The definition is validated when it is registered, so a misconfigured integration fails at
/// startup with the member that is wrong and how to fix it.
/// </summary>
/// <remarks>
/// Build the definition with an object initializer:
/// <code>
/// services.AddTransactionalIntegration(new TransactionalIntegrationDefinition&lt;OrderContext&gt;
/// {
///     DaprPubSubName = "pubsub",
///     DaprTopicName = "orders.received",
///     SourcePort = "orders-inbox",
///     ContextFactory = (metadata, isRetry) =&gt; new OrderContext(metadata, isRetry),
/// });
/// </code>
/// </remarks>
/// <typeparam name="TCtx">The context type shared by the receive and send pipelines.</typeparam>
public sealed class TransactionalIntegrationDefinition<TCtx> where TCtx : Context
{
    /// <summary>The name of the Dapr pub/sub component the receive side publishes swept files to.
    /// Required. (Member name mirrors
    /// <see cref="TransactionalIntegrationOptions.DaprPubSubName"/>.)</summary>
    public required string DaprPubSubName { get; init; }

    /// <summary>The name of the topic the receive side publishes swept files to, and the sidecar
    /// pushes back to the send side. Required. (Member name mirrors
    /// <see cref="TransactionalIntegrationOptions.DaprTopicName"/>.)</summary>
    public required string DaprTopicName { get; init; }

    /// <summary>The rest of the integration options: idle timeouts, message processing time, callback
    /// port. The definition's <see cref="DaprPubSubName"/> and <see cref="DaprTopicName"/> are
    /// authoritative — set through them, not here.</summary>
    public Action<TransactionalIntegrationOptions>? Configure { get; init; }

    /// <summary>The port the component sweeps, as declared in the system's topology; the
    /// registration calls <c>AddSourcePort</c> with it
    /// (and <see cref="Completion"/>), so the source the integration sweeps is described alongside
    /// the rest. Optional: leave null when the component registers the source port itself.</summary>
    public string? SourcePort { get; init; }

    /// <summary>What happens to a handled source file, for <see cref="SourcePort"/>. Default:
    /// deleted.</summary>
    public FileCompletion? Completion { get; init; }

    /// <summary>Creates the context for each source file and each message. Required when
    /// <typeparamref name="TCtx"/> derives from <see cref="Context"/>; when
    /// <typeparamref name="TCtx"/> is the base <see cref="Context"/>, leave null and the plain
    /// record is created.</summary>
    public ContextFactory<TCtx>? ContextFactory { get; init; }

    /// <summary>Optional runner settings: the job name (default: the component name) and the
    /// sidecar timeouts.</summary>
    public Action<JobOptions>? ConfigureJob { get; init; }

    /// <summary>Checks the definition and builds the validated integration options: the
    /// <see cref="Configure"/> tweaks with the definition's required pub/sub and topic applied last,
    /// so the final options are always the ones this definition validated.</summary>
    /// <returns>The integration options to register.</returns>
    /// <exception cref="InvalidOperationException"><see cref="DaprPubSubName"/> or
    /// <see cref="DaprTopicName"/> is missing, or the callback port is out of range.</exception>
    internal TransactionalIntegrationOptions Validate()
    {
        var options = new TransactionalIntegrationOptions();
        Configure?.Invoke(options);
        // The definition's required names are authoritative when the code set them: configuration
        // binds first, Configure runs second, and a set definition member beats both. An unset
        // member (the configuration-bound shape) defers to the options the merged Configure has
        // already bound — then the composed Subscription shape settles against the legacy members.
        if (NonEmpty(DaprPubSubName))
            options.DaprPubSubName = DaprPubSubName;
        if (NonEmpty(DaprTopicName))
            options.DaprTopicName = DaprTopicName;
        options.ConsolidateSubscription();
        if (string.IsNullOrEmpty(options.DaprPubSubName))
            throw new InvalidOperationException(
                "TransactionalIntegration composition failed: DaprPubSubName — the receive side needs the Dapr " +
                "pub/sub component to publish swept files to; set DaprPubSubName.");
        if (string.IsNullOrEmpty(options.DaprTopicName))
            throw new InvalidOperationException(
                "TransactionalIntegration composition failed: DaprTopicName — the receive side needs the topic to " +
                "publish swept files to (the send side consumes it); set DaprTopicName.");
        ComponentRegistration.EnsureValidCallbackPort(options.CallbackPort,
            nameof(TransactionalIntegrationOptions), "definition");
        return options;
    }

    /// <summary>The registration-ready context factory: the definition's own, or the default for the
    /// base <see cref="Context"/>.</summary>
    internal ContextFactory<TCtx> ResolveContextFactory() =>
        ContextFactory ?? ComponentRegistration.DefaultContextFactory<TCtx>(
            "TransactionalIntegration", nameof(ContextFactory));

    /// <summary>Builds the definition the configuration-based registration registers: the section's
    /// key values bind onto the options surface before the definition's own <see cref="Configure"/>
    /// runs, so code wins over configuration, and a definition member the factory sets stays
    /// authoritative over both. <see cref="Completion"/> binds only in code: it carries behavior,
    /// not a value configuration can express.</summary>
    /// <param name="configuration">The section holding the integration's values; the pub/sub and
    /// topic are read from <c>DaprPubSubName</c>/<c>DaprTopicName</c> or, when absent, from
    /// <c>PubSubName</c>/<c>TopicName</c>.</param>
    /// <param name="factory">Builds the definition the code owns — the context factory, the source
    /// port, and any value configuration must not overrule. Leave a member unset (null) to defer it
    /// to the configuration.</param>
    /// <returns>A definition equivalent to the factory's, with the configuration supplying the
    /// values the definition leaves unset.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> or
    /// <paramref name="factory"/> is null.</exception>
    internal static TransactionalIntegrationDefinition<TCtx> BoundTo(IConfiguration configuration,
        Func<TransactionalIntegrationDefinition<TCtx>> factory)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(factory);
        var source = factory();
        return new TransactionalIntegrationDefinition<TCtx>
        {
            // Unset members pass through: Validate() defers them to the options the merged
            // Configure has already bound the section's values onto.
            DaprPubSubName = source.DaprPubSubName,
            DaprTopicName = source.DaprTopicName,
            Configure = options =>
            {
                TransactionalIntegrationServiceCollectionExtensions.ApplyConfiguration(options, configuration);
                source.Configure?.Invoke(options);
            },
            SourcePort = source.SourcePort,
            Completion = source.Completion,
            ContextFactory = source.ContextFactory,
            ConfigureJob = source.ConfigureJob,
        };
    }

    private static bool NonEmpty(string? value) => !string.IsNullOrEmpty(value);
}
