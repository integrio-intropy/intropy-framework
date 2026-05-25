using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Blocks.TransactionalIntegration.Send.Steps;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Send;

/// <summary>
/// A builder class for the <see cref="SendPipeline{TInput,TOutput,TCtx}"/> class.
/// </summary>
/// <typeparam name="TInput">The type of POCO that enters the pipeline.</typeparam>
/// <typeparam name="TOutput">The type of the POCO that exists the pipeline.</typeparam>
/// <typeparam name="TCtx">The type of context used in the pipeline.</typeparam>
public class SendPipelineBuilder<TInput, TOutput, TCtx>
    where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;

    private DeserializeStep<TInput, TCtx>? _deserializer;
    private readonly List<ExtractStep<TInput, TCtx>> _extractors = [];
    private ValidateStep<TInput, TCtx>? _validator;
    private TransformStep<TInput, TOutput, TCtx>? _transformer;
    private SerializeStep<TOutput, TCtx>? _serializer;
    private SendStep<TCtx>? _sender;
    private IdempotencyCheckStep<TInput, TCtx>? _idempotencyChecker;
    private IdempotencyRecordStep<string, TCtx>? _idempotencyRecorder;
    private BusinessIncidentRouteStep<string, TCtx>? _businessIncidentRouter;

    private SendPipelineBuilder(string pipelineName, FrameworkOptions frameworkOptions,
        ILoggerFactory loggerFactory)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(frameworkOptions);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        _pipelineName = pipelineName;
        _frameworkOptions = frameworkOptions;
        _logger = loggerFactory.CreateLogger<SendPipeline<TInput, TOutput, TCtx>>();
    }

    /// <summary>
    /// Creates a new instance of <see cref="SendPipelineBuilder{TInput,TOutput,TCtx}"/>.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="frameworkOptions">An instance of <see cref="FrameworkOptions"/> used for framework configuration.</param>
    /// <param name="loggerFactory">An instance of <see cref="ILoggerFactory"/>.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="ArgumentException">Thrown if any required string arguments are empty.</exception>
    /// <exception cref="ArgumentNullException">Thrown if any required arguments are null.</exception>
    public static SendPipelineBuilder<TInput, TOutput, TCtx> Create(string pipelineName,
        FrameworkOptions frameworkOptions, ILoggerFactory loggerFactory)
    {
        return new SendPipelineBuilder<TInput, TOutput, TCtx>(pipelineName,
            frameworkOptions, loggerFactory);
    }

    /// <summary>
    /// Configures automatic idempotency handling using <see cref="IIdempotencyServiceClient"/>.
    /// This method configures both the check and recording steps.
    /// </summary>
    /// <param name="client">An instance of <see cref="IIdempotencyServiceClient"/> used for checking and recording idempotency.</param>
    /// <param name="idExtractor">Function to extract the unique ID from the payload.</param>
    /// <param name="dateExtractor">Function to extract the event date from the payload.</param>
    /// <param name="hashGenerator">Optional function to generate a hash of the payload. If not provided, uses a default implementation (JSON serialization + SHA256).</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any required arguments are null.</exception>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithIdempotency(
        IIdempotencyServiceClient client, Func<TInput, TCtx, string> idExtractor, Func<TInput, TCtx, DateTimeOffset> dateExtractor,
        Func<TInput, string>? hashGenerator = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(idExtractor);
        ArgumentNullException.ThrowIfNull(dateExtractor);

        // Create the checker step
        var checker = new ExternalIdempotencyChecker<TInput, TCtx>(client, _frameworkOptions, idExtractor,
            dateExtractor, hashGenerator);

        // Create the finalizer step
        var recorder =
            new ExternalIdempotencyRecorder<string, TCtx>(client, _frameworkOptions);

        // Configure both steps
        _idempotencyChecker = checker;
        _idempotencyRecorder = recorder;
        return this;
    }

    /// <summary>
    /// Use this instead of <see cref="WithIdempotency"/> to use custom implementations of <see cref="IdempotencyCheckStep{T,TCtx}"/> and <see cref="IdempotencyRecordStep{T,TCtx}"/>.
    /// </summary>
    /// <param name="idempotencyCheckStep">A custom implementation of <see cref="IdempotencyCheckStep{T,TCtx}"/></param>
    /// <param name="idempotencyRecorder">A custom implementation of <see cref="IdempotencyRecordStep{T,TCtx}"/></param>
    /// <returns></returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithCustomIdempotency(
        IdempotencyCheckStep<TInput, TCtx> idempotencyCheckStep,
        IdempotencyRecordStep<string, TCtx> idempotencyRecorder)
    {
        _idempotencyChecker = idempotencyCheckStep;
        _idempotencyRecorder = idempotencyRecorder;
        return this;
    }

    /// <summary>
    /// Configures deserialization using a custom deserializer.
    /// This step processes the raw string input to deserialize it into the input POCO type.
    /// </summary>
    /// <param name="deserializer">The <see cref="DeserializeStep{TInput, TContext}"/> that converts string to TInput.</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithDeserializer(
        DeserializeStep<TInput, TCtx> deserializer)
    {
        _deserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Adds extractors to the pipeline. Multiple extractors can be added and will be executed in order.
    /// </summary>
    /// <param name="extractor">The <see cref="ExtractStep{TInput,TContext}"/> that extracts additional data.</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithExtractor(
        ExtractStep<TInput, TCtx> extractor)
    {
        ArgumentNullException.ThrowIfNull(extractor);
        _extractors.Add(extractor);
        return this;
    }

    /// <summary>
    /// Configures input validation using a custom validator.
    /// </summary>
    /// <param name="validator">The <see cref="ValidateStep{TInput, TContext}"/> that validates TInput</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithValidator(
        ValidateStep<TInput, TCtx> validator)
    {
        _validator = validator;
        return this;
    }

    /// <summary>
    /// Configures transformation using a custom transformer step.
    /// This step transforms the validated input into the output type.
    /// </summary>
    /// <param name="transformer">The <see cref="TransformStep{TInput,TOutput, TContext}"/> that converts TInput to TOutput.</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithTransformer(
        TransformStep<TInput, TOutput, TCtx> transformer)
    {
        _transformer = transformer;
        return this;
    }

    /// <summary>
    /// Configures serialization using a custom serializer step.
    /// This step serializes the transformed output into a string.
    /// </summary>
    /// <param name="serializer">The <see cref="SerializeStep{TOutput, TContext}"/> that converts TOutput to string.</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithSerializer(
        SerializeStep<TOutput, TCtx> serializer)
    {
        _serializer = serializer;
        return this;
    }

    /// <summary>
    /// Configures sending using a custom sender.
    /// </summary>
    /// <param name="sender">The <see cref="SendStep{TContext}"/> that sends the processed data to the destination.</param>
    /// <returns>The builder for method chaining.</returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithSender(
        SendStep<TCtx> sender)
    {
        _sender = sender;
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
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="IBusinessIncidentServiceClient"/> is not registered in the service provider.</exception>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
        IBusinessIncidentServiceClient client,
        Func<TCtx, string> messageIdExtractor,
        Func<TCtx, string> subjectExtractor)
    {
        _businessIncidentRouter =
            new ExternalBusinessIncidentRouter<string, TCtx>(client, _frameworkOptions, messageIdExtractor,
                subjectExtractor, defaultValueFactory: () => "");

        return this;
    }

    /// <summary>
    /// Use this instead of <see cref="WithBusinessIncidents"/> to use custom implementations of <see cref="BusinessIncidentRouteStep{T,TCtx}"/>.
    /// </summary>
    /// <param name="businessIncidentRouteStep"></param>
    /// <returns></returns>
    public SendPipelineBuilder<TInput, TOutput, TCtx> WithCustomBusinessIncidents(
        BusinessIncidentRouteStep<string, TCtx> businessIncidentRouteStep)
    {
        _businessIncidentRouter = businessIncidentRouteStep;
        return this;
    }

    /// <summary>
    /// Builds the <see cref="SendPipeline{TInput,TOutput,TCtx}"/>.
    /// </summary>
    /// <returns>A configured instance of <see cref="SendPipeline{TInput,TOutput,TCtx}"/></returns>
    /// <exception cref="InvalidOperationException">Thrown when any of the required services are not configured.</exception>
    public SendPipeline<TInput, TOutput, TCtx> Build()
    {
        // Validate all required steps are set. Idempotency and business incident routing are optional and
        // are skipped at runtime when not configured (see SendPipeline using AddOptionalStep/AddOptionalFinalizer).
        if (_deserializer == null)
            throw new InvalidOperationException($"{nameof(DeserializeStep<,>)} must be configured");
        if (_validator == null) throw new InvalidOperationException($"{nameof(ValidateStep<,>)} must be configured");
        if (_transformer == null)
            throw new InvalidOperationException($"{nameof(TransformStep<,,>)} must be configured");
        if (_serializer == null) throw new InvalidOperationException($"{nameof(SerializeStep<,>)} must be configured");
        if (_sender == null) throw new InvalidOperationException($"{nameof(SendStep<>)} must be configured");

        return new SendPipeline<TInput, TOutput, TCtx>(
            _pipelineName,
            _logger,
            _deserializer,
            _extractors,
            _validator,
            _transformer,
            _serializer,
            _sender,
            _idempotencyChecker,
            _idempotencyRecorder,
            _businessIncidentRouter
        );
    }
}
