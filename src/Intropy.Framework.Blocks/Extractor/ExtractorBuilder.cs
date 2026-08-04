using CloudNative.CloudEvents;
using Dapr.Client;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Extractor;

/// <summary>
/// A pipeline template used for extractor components. It follows these steps:
/// <list type="number">
///   <item>Deserialize: deserializes the string data into a POCO</item>
///   <item>Validate: validates the POCO</item>
///   <item>Idempotency check: checks so that the same data has not been processed already</item>
///   <item>Transform: transforms the input POCO into an output POCO</item>
///   <item>Serialize: serializes the output POCO into string</item>
///   <item>Send: sends the string data to the destination</item>
///   <item>Record idempotency: records that the data has been processed</item>
///   <item>Route business incidents: routes any business incidents that occurred during the execution</item>
/// </list>
/// </summary>
/// <typeparam name="TInput"></typeparam>
/// <typeparam name="TOutput"></typeparam>
/// <typeparam name="TCtx"></typeparam>
public class ExtractorBuilder<TInput, TOutput, TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly IServiceProvider _serviceProvider;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;

    private DeserializeStep<TInput, TCtx>? _deserializer;
    private ValidateStep<TInput, TCtx>? _validator;
    private readonly List<ExtractStep<TInput, TCtx>> _extractors = new();
    private TransformStep<TInput, TOutput, TCtx>? _transformer;
    private SerializeStep<TOutput, TCtx>? _serializer;
    private SendStep<TCtx>? _sender;
    private IdempotencyCheckStep<TInput, TCtx>? _idempotencyChecker;
    private IdempotencyRecordStep<CloudEvent, TCtx>? _idempotencyRecorder;
    private BusinessIncidentRouteStep<CloudEvent, TCtx>? _businessIncidentRouter;

    /// <summary>
    /// Creates a new instance of <see>
    ///     <cref>Extractor{TInput,TOutput,TCtx}</cref>
    /// </see>
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">An instance of <see cref="IServiceProvider"/> to get required services from DI</param>
    /// <exception cref="ArgumentException">Thrown if <b>pipelineName</b> is null or empty</exception>
    /// <exception cref="ArgumentNullException">Thrown if any required parameters are null</exception>
    /// <exception cref="InvalidOperationException">Thrown if any required services are not registered in DI</exception>
    private ExtractorBuilder(string pipelineName, IServiceProvider serviceProvider)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        _pipelineName = pipelineName;
        _serviceProvider = serviceProvider;

        _logger = _serviceProvider.GetService<ILoggerFactory>()
                      ?.CreateLogger<Extractor<TInput, TOutput, TCtx>>() ??
                  throw new InvalidOperationException(
                      $"{nameof(ILoggerFactory)} is not registered in the service provider. Please register it using services.AddLogger()");

        _frameworkOptions = _serviceProvider.GetService<FrameworkOptions>()
                            ?? throw new InvalidOperationException(
                                $"{nameof(FrameworkOptions)} is not registered in the service provider. Please register it using services.{nameof(ServiceCollectionExtensions.AddIntropyFramework)}().");
    }

    /// <summary>
    /// Creates a new instance of <see cref="ExtractorBuilder{TInput,TOutput,TCtx}"/>
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">An instance of <see cref="IServiceProvider"/> used to get required services from DI.</param>
    /// <returns></returns>
    public static ExtractorBuilder<TInput, TOutput, TCtx> Create(string pipelineName,
        IServiceProvider serviceProvider)
    {
        return new ExtractorBuilder<TInput, TOutput, TCtx>(pipelineName, serviceProvider);
    }

    /// <summary>
    /// Configures deserialization using a custom deserializer.
    /// This step processes the raw string input to deserialize it into the input POCO type.
    /// </summary>
    /// <param name="deserializer">The <see cref="DeserializeStep{T,TCtx}"/> that converts string to TInput.</param>
    /// <returns>The builder for method chaining.</returns>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithDeserializer(
        DeserializeStep<TInput, TCtx> deserializer)
    {
        _deserializer = deserializer;
        return this;
    }

    /// <summary>
    /// Configures input validation using a custom validator.
    /// </summary>
    /// <param name="validator">The <see cref="ValidateStep{T,TCtx}"/> that validates TInput</param>
    /// <returns>The builder for method chaining.</returns>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithValidator(ValidateStep<TInput, TCtx> validator)
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
    public ExtractorBuilder<TInput, TOutput, TCtx> WithTransformer(
        TransformStep<TInput, TOutput, TCtx> transformer)
    {
        _transformer = transformer;
        return this;
    }

    /// <summary>
    /// Configures serialization using a custom serializer step.
    /// This step serializes the transformed output into a string.
    /// </summary>
    /// <param name="serializer">The <see cref="SerializeStep{T,TCtx}"/> that converts TOutput to string.</param>
    /// <returns>The builder for method chaining.</returns>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithSerializer(
        SerializeStep<TOutput, TCtx> serializer)
    {
        _serializer = serializer;
        return this;
    }

    /// <summary>
    /// Adds extractors to the pipeline. Multiple extractors can be added and will be executed in order.
    /// </summary>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithExtractor(ExtractStep<TInput, TCtx> extractor)
    {
        ArgumentNullException.ThrowIfNull(extractor);
        _extractors.Add(extractor);
        return this;
    }

    /// <summary>
    /// Configures sending using a custom sender.
    /// </summary>
    /// <param name="sender">The <see cref="SendStep{TCtx}"/> that sends the processed data to the destination.</param>
    /// <returns>The builder for method chaining.</returns>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TCtx> sender)
    {
        _sender = sender;
        return this;
    }

    /// <summary>
    /// Configures sending by resolving the registered <see cref="SendStep{TCtx}"/> from the service provider.
    /// Use this when the sender is registered in DI, so the "which sender?" decision lives in service
    /// registration alongside the other external edges, and tests can replace the registration with a fake.
    /// </summary>
    /// <remarks>
    /// Register the sender against the abstract base type, e.g.
    /// <c>services.AddSingleton&lt;SendStep&lt;MyContext&gt;&gt;(sp =&gt; new DaprTopicPublisher&lt;MyContext&gt;(...))</c>.
    /// The sender is resolved once at build time; register it as a singleton (or transient) rather than scoped.
    /// </remarks>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no <see cref="SendStep{TCtx}"/> is registered in the service provider.</exception>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithSenderFromServices()
    {
        _sender = _serviceProvider.GetService<SendStep<TCtx>>()
                  ?? throw new InvalidOperationException(
                      $"{nameof(SendStep<TCtx>)}<{typeof(TCtx).Name}> is not registered in the service provider. " +
                      $"Please register it using services.AddSingleton<{nameof(SendStep<TCtx>)}<{typeof(TCtx).Name}>>(...) " +
                      $"with a sender implementation (e.g. {nameof(DaprTopicPublisher<TCtx>)}).");
        return this;
    }

    /// <summary>
    /// Configures sending CloudEvents to a Dapr pub/sub topic.
    /// This is the recommended way to configure sending for extractors.
    /// The publisher will set the source and type on the CloudEvent from the configured values,
    /// serialize the event to JSON, and publish it to the specified topic.
    /// </summary>
    /// <param name="pubSubName">The name of the Dapr pub/sub component (e.g., "pubsub").</param>
    /// <param name="topicName">The topic to publish to (e.g., "customers").</param>
    /// <param name="source">The CloudEvent source URI identifying where the data came from (e.g., "urn:company:system:salesforce").</param>
    /// <param name="type">The CloudEvent type identifying the kind of event (e.g., "com.company.customer.extracted").</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="DaprClient"/> is not registered in the service provider.</exception>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithDaprTopicPublisher(
        string pubSubName,
        string topicName,
        Uri source,
        string type)
    {
        ArgumentException.ThrowIfNullOrEmpty(pubSubName);
        ArgumentException.ThrowIfNullOrEmpty(topicName);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrEmpty(type);

        var daprClient = _serviceProvider.GetService<DaprClient>()
                         ?? throw new InvalidOperationException(
                             $"{nameof(DaprClient)} is not registered in the service provider. Please register it using services.AddDaprClient().");

        _sender = new DaprTopicPublisher<TCtx>(daprClient, pubSubName, topicName, source, type);
        return this;
    }

    /// <summary>
    /// Configures sending CloudEvents to a Dapr service using service invocation.
    /// The publisher will set the source and type on the CloudEvent from the configured values,
    /// serialize the event to JSON, and invoke the target service's "ingest" endpoint with the CloudEvent as the request body.
    /// </summary>
    /// <param name="appId">The Dapr app ID of the target service.</param>
    /// <param name="source">The CloudEvent source URI identifying where the data came from (e.g., "urn:company:system:salesforce").</param>
    /// <param name="type">The CloudEvent type identifying the kind of event (e.g., "com.company.customer.extracted").</param>
    /// <param name="httpClientFactory">
    /// Factory that produces the <see cref="HttpClient"/> used to send the request through the Dapr sidecar.
    /// Receives the configured <see cref="IServiceProvider"/> so the factory can resolve dependencies from DI
    /// (e.g. <c>sp =&gt; sp.GetRequiredService&lt;IHttpClientFactory&gt;().CreateClient("dapr-invoke")</c>).
    /// The returned <see cref="HttpClient"/>'s lifetime is owned by the caller — the framework will not dispose it.
    /// For a Dapr-routed client, the factory can return <c>DaprClient.CreateInvokeHttpClient(appId)</c> from a
    /// host-managed singleton.
    /// </param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="DaprClient"/> is not registered in the service provider.</exception>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithDaprServiceInvoker(
        string appId,
        Uri source,
        string type,
        Func<IServiceProvider, HttpClient> httpClientFactory)
    {
        ArgumentException.ThrowIfNullOrEmpty(appId);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentNullException.ThrowIfNull(httpClientFactory);

        var daprClient = _serviceProvider.GetService<DaprClient>()
                         ?? throw new InvalidOperationException(
                             $"{nameof(DaprClient)} is not registered in the service provider. Please register it using services.AddDaprClient().");

        var httpClient = httpClientFactory(_serviceProvider);
        _sender = new DaprServiceInvoker<TCtx>(daprClient, httpClient, appId, source, type);
        return this;
    }

    /// <summary>
    /// Configures automatic business incident routing using dependency injection to resolve the <see cref="IBusinessIncidentServiceClient"/>.
    /// </summary>
    /// <param name="messageIdExtractor">Function to extract the messageId that should be used for business incidents.
    /// This ID should be representative of the current instance of the message being processed, and should be consistent across retries.</param>
    /// <param name="subjectExtractor">Function to extract the subject that should be used for business incidents.
    /// This should be an ID that one can use in discussion with a process owner.</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="IBusinessIncidentServiceClient"/> is not registered in the service provider.</exception>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
        Func<TCtx, string> messageIdExtractor, Func<TCtx, string> subjectExtractor)
    {
        var businessIncidentServiceClient = _serviceProvider.GetService<IBusinessIncidentServiceClient>() ??
                                            throw new InvalidOperationException(
                                                $"{nameof(IBusinessIncidentServiceClient)} is not registered in the service provider. Please register it using services.AddBusinessIncidentServiceClient() or register your own implementation.");

        _businessIncidentRouter =
            new ExternalBusinessIncidentRouter<CloudEvent, TCtx>(businessIncidentServiceClient,
                _frameworkOptions, messageIdExtractor, subjectExtractor, defaultValueFactory: () => new CloudEvent());

        return this;
    }

    /// <summary>
    /// Configures automatic idempotency handling using dependency injection to resolve the <see cref="IIdempotencyServiceClient"/>.
    /// This method configures both the check and recording steps.
    /// </summary>
    /// <param name="idExtractor">Function to extract the unique ID from the payload.</param>
    /// <param name="dateExtractor">Function to extract the event date from the payload.</param>
    /// <param name="hashGenerator">Optional function to generate a hash of the payload. If not provided, uses a default implementation (JSON serialization + SHA256).</param>
    /// <returns>The builder for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <see cref="IIdempotencyServiceClient"/> is not registered in the service provider.</exception>
    /// <exception cref="ArgumentNullException">Thrown when any of the required parameters are null.</exception>
    public ExtractorBuilder<TInput, TOutput, TCtx> WithIdempotency(
        Func<TInput, TCtx, string> idExtractor,
        Func<TInput, TCtx, DateTimeOffset> dateExtractor,
        Func<TInput, string>? hashGenerator = null)
    {
        ArgumentNullException.ThrowIfNull(idExtractor);
        ArgumentNullException.ThrowIfNull(dateExtractor);

        var client = _serviceProvider.GetService<IIdempotencyServiceClient>()
                     ?? throw new InvalidOperationException(
                         $"{nameof(IIdempotencyServiceClient)} is not registered in the service provider. Please register it using services.AddPipelineIdempotencyService() or register your own implementation.");

        // Create the checker step
        var checker =
            new ExternalIdempotencyChecker<TInput, TCtx>(client, _frameworkOptions, idExtractor, dateExtractor,
                hashGenerator);

        // Create the finalizer step
        var recorder = new ExternalIdempotencyRecorder<CloudEvent, TCtx>(client, _frameworkOptions);

        // Configure both steps
        _idempotencyChecker = checker;
        _idempotencyRecorder = recorder;
        return this;
    }

    /// <summary>
    /// Builds the <see cref="Extractor{TInput,TOutput,TContext}"/>.
    /// </summary>
    /// <returns>A configured instance of <see cref="Extractor{TInput,TOutput,TContext}"/></returns>
    /// <exception cref="InvalidOperationException">Thrown when any of the steps are not configured.</exception>
    public Extractor<TInput, TOutput, TCtx> Build()
    {
        // Validate all required steps are configured
        if (_deserializer == null)
            throw new InvalidOperationException("Deserialize step must be configured using WithDeserialization()");

        if (_validator == null)
            throw new InvalidOperationException("Validate step must be configured using WithValidation()");

        if (_idempotencyChecker == null)
            throw new InvalidOperationException("Idempotency must be configured using WithIdempotency()");

        if (_transformer == null)
            throw new InvalidOperationException("Transform step must be configured using WithTransformation()");

        if (_serializer == null)
            throw new InvalidOperationException("Serialize step must be configured using WithSerialization()");

        if (_sender == null)
            throw new InvalidOperationException(
                "Sender step must be configured using WithDaprTopicPublisher(), WithSender() or WithSenderFromServices()");

        if (_idempotencyRecorder == null)
            throw new InvalidOperationException("Idempotency must be configured using WithIdempotency()");

        if (_businessIncidentRouter == null)
            throw new InvalidOperationException(
                "Business incident route step must be configured using WithBusinessIncidents()");

        return new Extractor<TInput, TOutput, TCtx>(
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
