using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.BulkLoader;

/// <summary>
/// A fluent builder for creating <see cref="BulkLoader{TInput,TOutput,TCtx}"/> pipelines. Wires its
/// own steps independently of <see cref="Loader.LoaderBuilder{TInput,TOutput,TCtx}"/> — it does not
/// call into that builder's private wiring, even where the underlying step types are the same, so
/// neither builder's wiring can be pulled on by the other's needs.
/// </summary>
/// <remarks>
/// Unlike <see cref="Loader.LoaderBuilder{TInput,TOutput,TCtx}"/>, <see cref="WithIdempotency"/>
/// requires explicit id and date extractors rather than defaulting to CloudEvent attributes: a bulk
/// entry is not guaranteed to carry a per-entry CloudEvent subject or time the way a single-message
/// Loader's context does.
/// </remarks>
/// <typeparam name="TInput">The input type (deserialized from each message's payload).</typeparam>
/// <typeparam name="TOutput">The output type (sent to the external system).</typeparam>
/// <typeparam name="TCtx">The context type.</typeparam>
public class BulkLoaderBuilder<TInput, TOutput, TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly IServiceProvider _serviceProvider;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;

    private DeserializeStep<TInput, TCtx>? _deserializer;
    private readonly List<(ExtractStep<TInput, TCtx>? PerMessage, BatchExtractStep<TInput, TCtx>? Batch)> _extractors = [];
    private ValidateStep<TInput, TCtx>? _validator;
    private TransformStep<TInput, TOutput, TCtx>? _transformer;
    private SendStep<TOutput, TCtx>? _sender;
    private BatchSendStep<TOutput, TCtx>? _batchSender;
    private IdempotencyCheckStep<TInput, TCtx>? _idempotencyChecker;
    private IdempotencyRecordStep<TOutput, TCtx>? _idempotencyRecorder;
    private BusinessIncidentRouteStep<TOutput, TCtx>? _businessIncidentRouter;
    private SendReceiptStep<TOutput, TCtx>? _receiptSender;
    private int _degreeOfParallelism = 1;

    // The CloudEvent type of the route this pipeline serves in a routing bulk loader; null otherwise.
    private readonly string? _routeEventType;

    private BulkLoaderBuilder(string pipelineName, IServiceProvider serviceProvider, string? routeEventType)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        _pipelineName = pipelineName;
        _serviceProvider = serviceProvider;
        _routeEventType = routeEventType;

        _logger = _serviceProvider.GetService<ILoggerFactory>()
                      ?.CreateLogger<BulkLoader<TInput, TOutput, TCtx>>() ??
                  throw new InvalidOperationException(
                      $"{nameof(ILoggerFactory)} is not registered in the service provider. Please register it using services.AddLogger()");

        _frameworkOptions = _serviceProvider.GetService<FrameworkOptions>()
                            ?? throw new InvalidOperationException(
                                $"{nameof(FrameworkOptions)} is not registered in the service provider. Please register it using services.{nameof(ServiceCollectionExtensions.AddIntropyFramework)}().");
    }

    /// <summary>Creates a new instance of <see cref="BulkLoaderBuilder{TInput,TOutput,TCtx}"/>.</summary>
    public static BulkLoaderBuilder<TInput, TOutput, TCtx> Create(string pipelineName, IServiceProvider serviceProvider) =>
        new(pipelineName, serviceProvider, routeEventType: null);

    /// <summary>Creates a new instance of <see cref="BulkLoaderBuilder{TInput,TOutput,TCtx}"/> for
    /// one route of a bulk loader that routes several CloudEvent types to their own pipelines.</summary>
    public static BulkLoaderBuilder<TInput, TOutput, TCtx> Create(string pipelineName,
        IServiceProvider serviceProvider, string routeEventType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routeEventType);
        return new BulkLoaderBuilder<TInput, TOutput, TCtx>(pipelineName, serviceProvider, routeEventType);
    }

    /// <summary>Configures deserialization using a custom deserializer. Converts each message's
    /// payload bytes into TInput.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithDeserializer(DeserializeStep<TInput, TCtx> deserializer)
    {
        _deserializer = deserializer;
        return this;
    }

    /// <summary>Configures deserialization by resolving the registered <typeparamref name="TStep"/>
    /// from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithDeserializer<TStep>()
        where TStep : DeserializeStep<TInput, TCtx>
        => WithDeserializer(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Adds an extractor to the pipeline. Multiple extractors can be added and run in
    /// order, per message.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithExtractor(ExtractStep<TInput, TCtx> extractor)
    {
        ArgumentNullException.ThrowIfNull(extractor);
        _extractors.Add((extractor, null));
        return this;
    }

    /// <summary>Adds an extractor resolved from the service provider to the pipeline.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithExtractor<TStep>()
        where TStep : ExtractStep<TInput, TCtx>
        => WithExtractor(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Adds an extractor that runs once for the whole batch instead of once per message,
    /// such as a lookup that resolves many references in one call. Extractors run in the order they
    /// are added, whether per message or per batch.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBatchExtractor(BatchExtractStep<TInput, TCtx> extractor)
    {
        ArgumentNullException.ThrowIfNull(extractor);
        _extractors.Add((null, extractor));
        return this;
    }

    /// <summary>Adds a batch extractor resolved from the service provider to the pipeline.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBatchExtractor<TStep>()
        where TStep : BatchExtractStep<TInput, TCtx>
        => WithBatchExtractor(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Configures input validation using a custom validator.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithValidator(ValidateStep<TInput, TCtx> validator)
    {
        _validator = validator;
        return this;
    }

    /// <summary>Configures input validation by resolving the registered <typeparamref name="TStep"/>
    /// from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithValidator<TStep>()
        where TStep : ValidateStep<TInput, TCtx>
        => WithValidator(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Configures transformation using a custom transformer step.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithTransformer(TransformStep<TInput, TOutput, TCtx> transformer)
    {
        _transformer = transformer;
        return this;
    }

    /// <summary>Configures transformation by resolving the registered <typeparamref name="TStep"/>
    /// from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithTransformer<TStep>()
        where TStep : TransformStep<TInput, TOutput, TCtx>
        => WithTransformer(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Configures sending using a custom sender, called once per message. Replaces any
    /// sender configured before, per message or batch.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithSender(SendStep<TOutput, TCtx> sender)
    {
        ArgumentNullException.ThrowIfNull(sender);
        _sender = sender;
        _batchSender = null;
        return this;
    }

    /// <summary>Configures sending by resolving the registered <typeparamref name="TStep"/> from the
    /// service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithSender<TStep>()
        where TStep : SendStep<TOutput, TCtx>
        => WithSender(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Configures sending by resolving the registered <see cref="SendStep{T,TCtx}"/>
    /// from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithSenderFromServices() =>
        WithSender(_serviceProvider.GetService<SendStep<TOutput, TCtx>>()
                   ?? throw NotRegistered(nameof(SendStep<TOutput, TCtx>)));

    /// <summary>Configures sending using a custom batch sender. Sends every message's output to the
    /// external system in one call, instead of one call per message. Replaces any sender configured
    /// before, per message or batch.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBatchSender(BatchSendStep<TOutput, TCtx> sender)
    {
        ArgumentNullException.ThrowIfNull(sender);
        _batchSender = sender;
        _sender = null;
        return this;
    }

    /// <summary>Configures batch sending by resolving the registered <typeparamref name="TStep"/>
    /// from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBatchSender<TStep>()
        where TStep : BatchSendStep<TOutput, TCtx>
        => WithBatchSender(StepFromServices.Resolve<TStep>(_serviceProvider));

    /// <summary>Configures batch sending by resolving the registered
    /// <see cref="BatchSendStep{T,TCtx}"/> from the service provider.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBatchSenderFromServices() =>
        WithBatchSender(_serviceProvider.GetService<BatchSendStep<TOutput, TCtx>>()
                        ?? throw NotRegistered(nameof(BatchSendStep<TOutput, TCtx>)));

    private static InvalidOperationException NotRegistered(string step) =>
        new($"{step}<{typeof(TOutput).Name}, {typeof(TCtx).Name}> is not registered in the service provider. " +
            $"Please register it using services.AddSingleton<{step}<{typeof(TOutput).Name}, {typeof(TCtx).Name}>>(...) " +
            "with a sender implementation.");

    /// <summary>Configures an optional receipt sender that runs per message, after a successful load
    /// and, when idempotency is configured, after its record.</summary>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithReceiptSender(SendReceiptStep<TOutput, TCtx> receiptSender)
    {
        ArgumentNullException.ThrowIfNull(receiptSender);
        _receiptSender = receiptSender;
        return this;
    }

    /// <summary>Configures automatic business incident routing using dependency injection to resolve
    /// the <see cref="IBusinessIncidentServiceClient"/>.</summary>
    /// <param name="messageIdExtractor">Function to extract the messageId that should be used for
    /// business incidents. This ID should be representative of the current instance of the message
    /// being processed, and should be consistent across retries.</param>
    /// <param name="subjectExtractor">Function to extract the subject that should be used for
    /// business incidents. This should be an ID that one can use in discussion with a process
    /// owner.</param>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithBusinessIncidents(
        Func<TCtx, string> messageIdExtractor, Func<TCtx, string> subjectExtractor)
    {
        var client = _serviceProvider.GetService<IBusinessIncidentServiceClient>() ??
                     throw new InvalidOperationException(
                         $"{nameof(IBusinessIncidentServiceClient)} is not registered in the service provider. Please register it using services.AddBusinessIncidentServiceClient() or register your own implementation.");

        _businessIncidentRouter = new ExternalBusinessIncidentRouter<TOutput, TCtx>(
            client, _frameworkOptions, messageIdExtractor, subjectExtractor, defaultValueFactory: () => default!);

        return this;
    }

    /// <summary>
    /// Configures automatic idempotency handling using an <see cref="IIdempotencyServiceClient"/>
    /// resolved from the service provider. The hash is generated from the TInput payload. Optional:
    /// without it, the check and record steps are skipped, no <see cref="IIdempotencyServiceClient"/>
    /// needs to be registered.
    /// </summary>
    /// <remarks>
    /// In a bulk loader that routes several event types, <paramref name="scope"/> decides whether
    /// this route's idempotency records are its own (<see cref="IdempotencyScope.Route"/>, the
    /// default: the id is <c>{event type}:{id}</c>) or shared with every route over the same id
    /// (<see cref="IdempotencyScope.Entity"/>).
    /// </remarks>
    /// <param name="idExtractor">Function that returns the idempotency ID from the deserialized
    /// payload and the message's context.</param>
    /// <param name="dateExtractor">Function that returns the event date from the deserialized
    /// payload and the message's context.</param>
    /// <param name="hashGenerator">Optional function to generate a hash of the payload. If not
    /// provided, the payload's <see cref="Intropy.Framework.Core.Pipeline.Abstractions.IHashable.GetHashString"/>
    /// when it implements <see cref="Intropy.Framework.Core.Pipeline.Abstractions.IHashable"/>,
    /// otherwise its JSON serialization, is hashed with SHA-256.</param>
    /// <param name="scope">Which records this route's idempotency is checked against.</param>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithIdempotency(
        Func<TInput, TCtx, string> idExtractor, Func<TInput, TCtx, DateTimeOffset> dateExtractor,
        Func<TInput, string>? hashGenerator = null, IdempotencyScope scope = IdempotencyScope.Route)
    {
        ArgumentNullException.ThrowIfNull(idExtractor);
        ArgumentNullException.ThrowIfNull(dateExtractor);

        // Guard the caller's id before it is prefixed: "{event type}:" alone would pass the checker's own empty check.
        var customId = idExtractor;
        idExtractor = (input, context) => customId(input, context) is { Length: > 0 } id
            ? id
            : throw new InvalidOperationException("The idempotency ID extractor passed to WithIdempotency returned an empty ID.");

        if (scope == IdempotencyScope.Route && _routeEventType is { } eventType)
        {
            var recordId = idExtractor;
            idExtractor = (input, context) => $"{eventType}:{recordId(input, context)}";
        }

        var client = _serviceProvider.GetService<IIdempotencyServiceClient>()
                     ?? throw new InvalidOperationException(
                         $"{nameof(IIdempotencyServiceClient)} is not registered in the service provider. Please register it using services.AddPipelineIdempotencyService() or register your own implementation.");

        _idempotencyChecker = new ExternalIdempotencyChecker<TInput, TCtx>(
            client, _frameworkOptions, idExtractor, dateExtractor, hashGenerator);
        _idempotencyRecorder = new ExternalIdempotencyRecorder<TOutput, TCtx>(client, _frameworkOptions);
        return this;
    }

    /// <summary>Configures the maximum number of messages processed concurrently by the steps that
    /// run per message. Defaults to 1: those steps handle one message at a time, and only the batch
    /// steps handle the messages together.</summary>
    /// <param name="degreeOfParallelism">How many messages the per-message steps run at once.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="degreeOfParallelism"/> is below
    /// 1.</exception>
    public BulkLoaderBuilder<TInput, TOutput, TCtx> WithParallelism(int degreeOfParallelism)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(degreeOfParallelism, 1);
        _degreeOfParallelism = degreeOfParallelism;
        return this;
    }

    /// <summary>Builds the <see cref="BulkLoader{TInput,TOutput,TCtx}"/>.</summary>
    /// <exception cref="InvalidOperationException">Thrown when any required step is not configured.</exception>
    public BulkLoader<TInput, TOutput, TCtx> Build()
    {
        if (_deserializer == null)
            throw new InvalidOperationException("Deserialize step must be configured using WithDeserializer()");

        if (_validator == null)
            throw new InvalidOperationException("Validate step must be configured using WithValidator()");

        if (_transformer == null)
            throw new InvalidOperationException("Transform step must be configured using WithTransformer()");

        if (_sender == null && _batchSender == null)
            throw new InvalidOperationException(
                "Sender step must be configured using WithSender() or WithBatchSender()");

        if (_businessIncidentRouter == null)
            throw new InvalidOperationException(
                "Business incident route step must be configured using WithBusinessIncidents()");

        return new BulkLoader<TInput, TOutput, TCtx>(
            _pipelineName,
            _logger,
            _deserializer,
            _extractors,
            _validator,
            _transformer,
            _sender,
            _batchSender,
            _idempotencyChecker,
            _idempotencyRecorder,
            _businessIncidentRouter,
            _receiptSender,
            _degreeOfParallelism
        );
    }
}
