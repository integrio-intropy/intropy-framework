using System.Globalization;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// A fluent builder for creating Loader pipelines. It follows these steps:
/// <list type="number">
///   <item>Deserialize: deserializes the CloudEvent data into a POCO, extracts CloudEvent metadata to context</item>
///   <item>Extract: extracts data from the input</item>
///   <item>Validate: validates the POCO</item>
///   <item>Idempotency check: checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Send: sends the data to the external system</item>
///   <item>Record idempotency: records that the data has been processed</item>
///   <item>Send receipt (optional): sends a receipt after a successful load</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <typeparam name="TInput">The input type (deserialized from CloudEvent.Data)</typeparam>
/// <typeparam name="TOutput">The output type (sent to external system)</typeparam>
/// <typeparam name="TCtx">The context type</typeparam>
public class LoaderBuilder<TInput, TOutput, TCtx> where TCtx : Context
{

    private readonly string _pipelineName;
    private readonly IServiceProvider _serviceProvider;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;

    private DeserializeStep<TInput, TCtx>? _deserializer;
    private readonly List<ExtractStep<TInput, TCtx>> _extractors = new();
    private ValidateStep<TInput, TCtx>? _validator;
    private TransformStep<TInput, TOutput, TCtx>? _transformer;
    private SendStep<TOutput, TCtx>? _sender;
    private IdempotencyCheckStep<TInput, TCtx>? _idempotencyChecker;
    private IdempotencyRecordStep<TOutput, TCtx>? _idempotencyRecorder;
    private BusinessIncidentRouteStep<TOutput, TCtx>? _businessIncidentRouter;

    // Optional receipt sender
    private SendStep<TOutput, TCtx>? _receiptSender;

    /// <summary>
    /// Creates a new instance of <see>
    ///     <cref>LoaderBuilder{TInput,TOutput,TCtx}</cref>
    /// </see>
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">An instance of <see cref="IServiceProvider"/> to get required services from DI</param>
    /// <exception cref="ArgumentException">Thrown if <b>pipelineName</b> is null or empty</exception>
    /// <exception cref="ArgumentNullException">Thrown if any required parameters are null</exception>
    /// <exception cref="InvalidOperationException">Thrown if any required services are not registered in DI</exception>
    private LoaderBuilder(string pipelineName, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        _pipelineName = pipelineName;
        _serviceProvider = serviceProvider;

        _logger = _serviceProvider.GetService<ILoggerFactory>()
                      ?.CreateLogger<Loader<TInput, TOutput, TCtx>>() ??
                  throw new InvalidOperationException(
                      $"{nameof(ILoggerFactory)} is not registered in the service provider. Please register it using services.AddLogger()");

        _frameworkOptions = _serviceProvider.GetService<FrameworkOptions>()
                            ?? throw new InvalidOperationException(
                                $"{nameof(FrameworkOptions)} is not registered in the service provider. Please register it using services.{nameof(ServiceCollectionExtensions.AddIntropyFramework)}().");
    }

    /// <summary>
    /// Creates a new instance of <see cref="LoaderBuilder{TInput,TOutput,TCtx}"/>
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">An instance of <see cref="IServiceProvider"/> used to get required services from DI.</param>
    /// <returns></returns>
    public static LoaderBuilder<TInput, TOutput, TCtx> Create(string pipelineName,
        IServiceProvider serviceProvider)
    {
        return new LoaderBuilder<TInput, TOutput, TCtx>(pipelineName, serviceProvider);
    }

    /// <summary>
    /// Configures deserialization using a custom deserializer.
    /// This step deserializes the CloudEvent.Data into TInput and extracts CloudEvent metadata to context.
    /// </summary>
    /// <param name="deserializer">The <see cref="DeserializeStep{T,TCtx}"/> that converts CloudEvent to TInput.</param>
    /// <returns>The builder for method chaining.</returns>
    public LoaderBuilder<TInput, TOutput, TCtx> WithDeserializer(
        DeserializeStep<TInput, TCtx> deserializer)
    {
        _deserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Adds extractors to the pipeline. Multiple extractors can be added and will be executed in order.
    /// </summary>
    public LoaderBuilder<TInput, TOutput, TCtx> WithExtractor(ExtractStep<TInput, TCtx> extractor)
    {
        ArgumentNullException.ThrowIfNull(extractor);
        _extractors.Add(extractor);
        return this;
    }

    /// <summary>
    /// Configures input validation using a custom validator.
    /// </summary>
    /// <param name="validator">The <see cref="ValidateStep{T,TCtx}"/> that validates TInput</param>
    /// <returns>The builder for method chaining.</returns>
    public LoaderBuilder<TInput, TOutput, TCtx> WithValidator(ValidateStep<TInput, TCtx> validator)
    {
        _validator = validator;
        return this;
    }

    /// <summary>
    /// Configures transformation using a custom transformer step.
    /// This step transforms the validated input into the output type.
    /// </summary>
    /// <param name="transformer">The <see cref="TransformStep{TInput,TOutput,TCtx}"/> that converts TInput to TOutput.</param>
    /// <returns>The builder for method chaining.</returns>
    public LoaderBuilder<TInput, TOutput, TCtx> WithTransformer(
        TransformStep<TInput, TOutput, TCtx> transformer)
    {
        _transformer = transformer;
        return this;
    }

    /// <summary>
    /// Configures sending using a custom sender.
    /// This step sends the transformed data to the external system.
    /// </summary>
    /// <param name="sender">The <see cref="SendStep{T,TCtx}"/> that sends data to the external system.</param>
    /// <returns>The builder for method chaining.</returns>
    public LoaderBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TOutput, TCtx> sender)
    {
        _sender = sender;
        return this;
    }

    /// <summary>
    /// Configures sending by resolving the registered <see cref="SendStep{T,TCtx}"/> from the service provider.
    /// Use this when the sender is registered in DI, so the "which sender?" decision lives in service
    /// registration alongside the other external edges, and tests can replace the registration with a fake.
    /// </summary>
    /// <remarks>
    /// Register the sender against the abstract base type, e.g.
    /// <c>services.AddSingleton&lt;SendStep&lt;MyOutput, MyContext&gt;&gt;(sp =&gt; new MySender(...))</c>.
    /// The sender is resolved once at build time; register it as a singleton (or transient) rather than scoped.
    /// </remarks>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no <see cref="SendStep{T,TCtx}"/> is registered in the service provider.</exception>
    public LoaderBuilder<TInput, TOutput, TCtx> WithSenderFromServices()
    {
        _sender = _serviceProvider.GetService<SendStep<TOutput, TCtx>>()
                  ?? throw new InvalidOperationException(
                      $"{nameof(SendStep<TOutput, TCtx>)}<{typeof(TOutput).Name}, {typeof(TCtx).Name}> is not registered in the service provider. " +
                      $"Please register it using services.AddSingleton<{nameof(SendStep<TOutput, TCtx>)}<{typeof(TOutput).Name}, {typeof(TCtx).Name}>>(...) " +
                      "with a sender implementation.");
        return this;
    }

    /// <summary>
    /// Configures an optional receipt sender that runs after a successful load and idempotency record.
    /// The receipt sender receives the pipeline output (TOutput) and can publish it in any format
    /// (e.g., CloudEvent to a Dapr topic, HTTP call, etc.).
    /// </summary>
    /// <param name="receiptSender">The <see cref="SendStep{T,TCtx}"/> that sends the receipt.</param>
    /// <returns>The builder for method chaining.</returns>
    public LoaderBuilder<TInput, TOutput, TCtx> WithReceiptSender(
        SendStep<TOutput, TCtx> receiptSender)
    {
        ArgumentNullException.ThrowIfNull(receiptSender);
        _receiptSender = receiptSender;
        return this;
    }

    /// <summary>
    /// Configures automatic business incident routing using dependency injection to resolve the <see cref="IBusinessIncidentServiceClient"/>.
    /// </summary>
    /// <param name="messageIdExtractor">Function to extract the messageId that should be used for business incidents.
    /// This ID should be representative of the current instance of the message being processed, and should be consistent across retries.</param>
    /// <param name="subjectExtractor">Function to extract the subject that should be used for business incidents.
    /// This should be an ID that one can use in discussion with a process owner.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="IBusinessIncidentServiceClient"/> is not registered in the service provider.</exception>
    public LoaderBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
        Func<TCtx, string> messageIdExtractor, Func<TCtx, string> subjectExtractor)
    {
        var businessIncidentServiceClient = _serviceProvider.GetService<IBusinessIncidentServiceClient>() ??
                                            throw new InvalidOperationException(
                                                $"{nameof(IBusinessIncidentServiceClient)} is not registered in the service provider. Please register it using services.AddBusinessIncidentServiceClient() or register your own implementation.");

        _businessIncidentRouter =
            new ExternalBusinessIncidentRouter<TOutput, TCtx>(businessIncidentServiceClient,
                _frameworkOptions, messageIdExtractor, subjectExtractor, defaultValueFactory: () => default!);

        return this;
    }

    /// <summary>
    /// Configures automatic idempotency handling using CloudEvent metadata from context.
    /// Uses CloudEvent.Subject as the ID and CloudEvent.Time as the date.
    /// The hash is generated from the TInput payload.
    /// </summary>
    /// <param name="hashGenerator">Optional function to generate a hash of the payload. If not provided, uses a default implementation (property reflection + SHA256).</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="IIdempotencyServiceClient"/> is not registered in the service provider.</exception>
    public LoaderBuilder<TInput, TOutput, TCtx> WithIdempotency(
        Func<TInput, string>? hashGenerator = null)
    {
        var client = _serviceProvider.GetService<IIdempotencyServiceClient>()
                     ?? throw new InvalidOperationException(
                         $"{nameof(IIdempotencyServiceClient)} is not registered in the service provider. Please register it using services.AddPipelineIdempotencyService() or register your own implementation.");

        var checker = new ExternalIdempotencyChecker<TInput, TCtx>(
            client, _frameworkOptions, IdExtractor, DateExtractor, hashGenerator);
        var recorder = new ExternalIdempotencyRecorder<TOutput, TCtx>(client, _frameworkOptions);

        _idempotencyChecker = checker;
        _idempotencyRecorder = recorder;
        return this;

        DateTimeOffset DateExtractor(TInput _, TCtx context)
        {
            if (!context.Metadata.TryGetValue(CloudEventContextKeys.Time, out var timeStr) || string.IsNullOrEmpty(timeStr)) throw new InvalidOperationException($"Missing required CloudEvent.Time in context (key: '{CloudEventContextKeys.Time}'). " + "Ensure the CloudEvent has a Time set.");

            if (!DateTimeOffset.TryParse(timeStr, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var eventDate)) throw new InvalidOperationException($"Invalid RFC 3339 date format in CloudEvent.Time: '{timeStr}'");

            return eventDate;
        }

        // Define extractors that read CloudEvent metadata from context
        string IdExtractor(TInput _, TCtx context)
        {
            if (!context.Metadata.TryGetValue(CloudEventContextKeys.Subject, out var id) || string.IsNullOrEmpty(id)) throw new InvalidOperationException($"Missing required CloudEvent.Subject in context (key: '{CloudEventContextKeys.Subject}'). " + "Ensure the CloudEvent has a Subject set.");
            return id;
        }
    }

    /// <summary>
    /// Builds the <see cref="Loader{TInput,TOutput,TContext}"/>.
    /// </summary>
    /// <returns>A configured instance of <see cref="Loader{TInput,TOutput,TContext}"/></returns>
    /// <exception cref="InvalidOperationException">Thrown when any of the steps are not configured.</exception>
    public Loader<TInput, TOutput, TCtx> Build()
    {
        // Validate all required steps are configured
        if (_deserializer == null)
            throw new InvalidOperationException("Deserialize step must be configured using WithDeserializer()");

        if (_validator == null)
            throw new InvalidOperationException("Validate step must be configured using WithValidator()");

        if (_idempotencyChecker == null)
            throw new InvalidOperationException("Idempotency must be configured using WithIdempotency()");

        if (_transformer == null)
            throw new InvalidOperationException("Transform step must be configured using WithTransformer()");

        if (_sender == null)
            throw new InvalidOperationException(
                "Sender step must be configured using WithSender() or WithSenderFromServices()");

        if (_idempotencyRecorder == null)
            throw new InvalidOperationException("Idempotency must be configured using WithIdempotency()");

        if (_businessIncidentRouter == null)
            throw new InvalidOperationException(
                "Business incident route step must be configured using WithBusinessIncidents()");

        return new Loader<TInput, TOutput, TCtx>(
            _pipelineName,
            _logger,
            _deserializer,
            _extractors,
            _validator,
            _transformer,
            _sender,
            _idempotencyChecker,
            _idempotencyRecorder,
            _businessIncidentRouter,
            _receiptSender
        );
    }
}
