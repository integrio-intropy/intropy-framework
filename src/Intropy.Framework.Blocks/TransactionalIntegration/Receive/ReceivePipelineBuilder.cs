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

    private ReceiveStep<TCtx>? _receiver;
    private EnqueueStep<TCtx>? _enqueuer;
    private CompleteStep<TCtx>? _completer;
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
    /// Configures the receive step that reads content from the source.
    /// </summary>
    /// <param name="receiver">The <see cref="ReceiveStep{TCtx}"/> that reads content.</param>
    /// <returns>The builder for method chaining.</returns>
    public ReceivePipelineBuilder<TCtx> WithReceiver(ReceiveStep<TCtx> receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        _receiver = receiver;
        return this;
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
    /// Configures the complete step that handles cleanup.
    /// </summary>
    /// <param name="completer">The <see cref="CompleteStep{TCtx}"/> that handles cleanup.</param>
    /// <returns>The builder for method chaining.</returns>
    public ReceivePipelineBuilder<TCtx> WithCompleter(CompleteStep<TCtx> completer)
    {
        ArgumentNullException.ThrowIfNull(completer);
        _completer = completer;
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
        // Business incident routing is optional and is skipped at runtime when not configured
        // (see ReceivePipeline using AddOptionalFinalizer).
        if (_receiver == null)
            throw new InvalidOperationException($"{nameof(ReceiveStep<TCtx>)} must be configured");
        if (_enqueuer == null)
            throw new InvalidOperationException($"{nameof(EnqueueStep<TCtx>)} must be configured");
        if (_completer == null)
            throw new InvalidOperationException($"{nameof(CompleteStep<TCtx>)} must be configured");

        return new ReceivePipeline<TCtx>(
            _pipelineName,
            _logger,
            _receiver,
            _enqueuer,
            _completer,
            _businessIncidentRouter
        );
    }
}
