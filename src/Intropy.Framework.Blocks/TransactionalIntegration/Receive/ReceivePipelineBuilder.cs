using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive;

/// <summary>
/// A builder class for the <see cref="ReceivePipeline{TCtx}"/> class.
/// </summary>
/// <typeparam name="TCtx">The type of context used in the pipeline.</typeparam>
public class ReceivePipelineBuilder<TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;

    private EnqueueStep<TCtx>? _enqueuer;
    private (IFileAdapter Store, ClaimCheckPolicy Policy)? _claimCheck;
    private BusinessIncidentRouteStep<SourceItem, TCtx>? _businessIncidentRouter;

    private ReceivePipelineBuilder(string pipelineName, FrameworkOptions frameworkOptions,
        ILoggerFactory loggerFactory)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(frameworkOptions);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        _pipelineName = pipelineName;
        _frameworkOptions = frameworkOptions;
        _logger = loggerFactory.CreateLogger<ReceivePipeline<TCtx>>();
    }

    /// <summary>
    /// Creates a new instance of <see cref="ReceivePipelineBuilder{TCtx}"/>.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="frameworkOptions">An instance of <see cref="FrameworkOptions"/> used for framework configuration.</param>
    /// <param name="loggerFactory">An instance of <see cref="ILoggerFactory"/>.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="ArgumentException">Thrown if any required string arguments are empty.</exception>
    /// <exception cref="ArgumentNullException">Thrown if any required arguments are null.</exception>
    public static ReceivePipelineBuilder<TCtx> Create(string pipelineName,
        FrameworkOptions frameworkOptions, ILoggerFactory loggerFactory)
    {
        return new ReceivePipelineBuilder<TCtx>(pipelineName, frameworkOptions, loggerFactory);
    }

    /// <summary>
    /// Configures the enqueue step that publishes content to the queue.
    /// </summary>
    /// <param name="enqueuer">The <see cref="EnqueueStep{TCtx}"/> that publishes content.</param>
    /// <returns>The builder for method chaining.</returns>
    public ReceivePipelineBuilder<TCtx> WithEnqueuer(EnqueueStep<TCtx> enqueuer)
    {
        ArgumentNullException.ThrowIfNull(enqueuer);
        _enqueuer = enqueuer;
        return this;
    }

    /// <summary>
    /// Configures the claim check: a payload too large for the broker is written to
    /// <paramref name="store"/> before the enqueuer runs, and the message carries a pointer to it
    /// instead of the payload. Optional; without it every payload rides inside its envelope, as before.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Internal because the claim check has one control surface, and it is configuration: the
    /// hosting layer calls this from the registered claim store (<c>AddClaimStore</c>) and the
    /// integration's <c>ClaimCheckMode</c> / <c>ClaimCheckThresholdBytes</c>. A pipeline cannot
    /// configure its own, which is what makes <see cref="ClaimCheckMode.Never"/> a rollback an
    /// operator can actually reach for.
    /// </para>
    /// <para>The send side resolves the pointer on its own, from the claim store registered on the
    /// component — there is nothing to configure there.</para>
    /// </remarks>
    /// <param name="store">The claim store to write payloads to.</param>
    /// <param name="policy">When a payload is claim-checked: above a threshold, or always.</param>
    /// <returns>The builder for method chaining.</returns>
    internal ReceivePipelineBuilder<TCtx> WithClaimCheck(IFileAdapter store, ClaimCheckPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(policy);
        // The component name is half of every claim key and is known now, so an unusable one fails
        // when the pipeline is composed rather than on the first payload large enough to be filed.
        ClaimWriter.RequireComponentCanName(_frameworkOptions);
        // Kept rather than composed: the enqueuer it wraps may not be configured yet, so the two
        // With* calls stay order-independent and Build() does the wrapping.
        _claimCheck = (store, policy);
        return this;
    }

    /// <summary>
    /// Configures automatic business incident routing using the <see cref="IBusinessIncidentServiceClient"/>.
    /// </summary>
    /// <param name="client">An instance of <see cref="IBusinessIncidentServiceClient"/> used to route business incidents.</param>
    /// <param name="messageIdExtractor">Function to extract the messageId that should be used for business incidents.
    /// This ID should be representative of the current instance of the message being processed, and should be consistent across retries.</param>
    /// <param name="subjectExtractor">Function to extract the subject that should be used for business incidents.
    /// This should be an ID that one can use in discussion with a process owner.</param>
    /// <returns>The builder for method chaining.</returns>
    public ReceivePipelineBuilder<TCtx> WithBusinessIncidents(
        IBusinessIncidentServiceClient client,
        Func<TCtx, string> messageIdExtractor,
        Func<TCtx, string> subjectExtractor)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(messageIdExtractor);

        _businessIncidentRouter =
            new ExternalBusinessIncidentRouter<SourceItem, TCtx>(client,
                _frameworkOptions,
                messageIdExtractor,
                subjectExtractor,
                defaultValueFactory: () => new SourceItem("", []));

        return this;
    }

    /// <summary>
    /// Use this instead of <see cref="WithBusinessIncidents"/> to use custom implementations
    /// of <see cref="BusinessIncidentRouteStep{T,TCtx}"/>.
    /// </summary>
    /// <param name="businessIncidentRouteStep">A custom implementation of <see cref="BusinessIncidentRouteStep{T,TCtx}"/>.</param>
    /// <returns>The builder for method chaining.</returns>
    public ReceivePipelineBuilder<TCtx> WithCustomBusinessIncidents(
        BusinessIncidentRouteStep<SourceItem, TCtx> businessIncidentRouteStep)
    {
        ArgumentNullException.ThrowIfNull(businessIncidentRouteStep);
        _businessIncidentRouter = businessIncidentRouteStep;
        return this;
    }

    /// <summary>
    /// Builds the <see cref="ReceivePipeline{TCtx}"/>.
    /// </summary>
    /// <returns>A configured instance of <see cref="ReceivePipeline{TCtx}"/>.</returns>
    /// <exception cref="InvalidOperationException">Thrown when any of the required steps are not configured.</exception>
    public ReceivePipeline<TCtx> Build()
    {
        // Business incident routing is optional and skipped at runtime when not configured (see
        // ReceivePipeline using AddOptionalFinalizer). The claim check is optional in a different
        // way: without one the enqueuer is simply not wrapped.
        if (_enqueuer == null)
            throw new InvalidOperationException($"{nameof(EnqueueStep<TCtx>)} must be configured");

        return new ReceivePipeline<TCtx>(
            _pipelineName,
            _logger,
            ClaimChecked(_enqueuer),
            _businessIncidentRouter
        );
    }

    /// <summary><paramref name="enqueuer"/> wrapped in the configured claim check, or unchanged when
    /// none is configured.</summary>
    private EnqueueStep<TCtx> ClaimChecked(EnqueueStep<TCtx> enqueuer) =>
        _claimCheck is { } claimCheck
            ? new ClaimCheckEnqueue<TCtx>(enqueuer, claimCheck.Store, claimCheck.Policy, _frameworkOptions)
            : enqueuer;
}
