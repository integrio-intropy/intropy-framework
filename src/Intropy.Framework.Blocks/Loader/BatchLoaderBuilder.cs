using System.Globalization;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.Shared.Steps;
using Intropy.Framework.Blocks.Shared.Steps.External;
using Intropy.Framework.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Blocks.Loader;

/// <summary>
/// A fluent builder for <see cref="BatchLoader{TInput,TEnriched,TOutput,TCtx}"/>: a loader that runs a
/// whole delivered batch at once — per-entry deserialize and filter, coalescing by entity key, an
/// optional lookup per chunk of keys, then the per-entity validate → idempotency → transform → send →
/// record → incidents steps with bounded concurrency.
/// </summary>
/// <typeparam name="TInput">The deserialized event.</typeparam>
/// <typeparam name="TEnriched">What the per-entity steps run on: the looked-up state
/// (<see cref="WithLookup"/>), or <typeparamref name="TInput"/> itself without a lookup.</typeparam>
/// <typeparam name="TOutput">What is sent to the external system.</typeparam>
/// <typeparam name="TCtx">The context type.</typeparam>
public sealed class BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> where TCtx : Context
{
    private readonly string _pipelineName;
    private readonly IServiceProvider _serviceProvider;
    private readonly FrameworkOptions _frameworkOptions;
    private readonly ILogger _logger;
    private readonly string? _routeEventType;

    private DeserializeStep<TInput, TCtx>? _deserializer;
    private Func<TInput, bool>? _filter;
    private Func<TInput, string>? _keySelector;
    private BatchLookupStep<TEnriched>? _lookup;
    private int _chunkSize = 100;
    private MissingItemPolicy _missing = MissingItemPolicy.Retry;
    private ValidateStep<TEnriched, TCtx>? _validator;
    private TransformStep<TEnriched, TOutput, TCtx>? _transformer;
    private SendStep<TOutput, TCtx>? _sender;
    private IdempotencyCheckStep<TEnriched, TCtx>? _idempotencyChecker;
    private IdempotencyRecordStep<TOutput, TCtx>? _idempotencyRecorder;
    private BusinessIncidentRouteStep<TOutput, TCtx>? _businessIncidentRouter;
    private int _maxConcurrency = 1;

    private BatchLoaderBuilder(string pipelineName, IServiceProvider serviceProvider, string? routeEventType)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineName);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        _pipelineName = pipelineName;
        _serviceProvider = serviceProvider;
        _routeEventType = routeEventType;
        _logger = serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<BatchLoader<TInput, TEnriched, TOutput, TCtx>>()
                  ?? throw new InvalidOperationException(
                      $"{nameof(ILoggerFactory)} is not registered in the service provider. Please register it using services.AddLogging()");
        _frameworkOptions = serviceProvider.GetService<FrameworkOptions>()
                            ?? throw new InvalidOperationException(
                                $"{nameof(FrameworkOptions)} is not registered in the service provider. Please register it using services.{nameof(ServiceCollectionExtensions.AddIntropyFramework)}().");
    }

    /// <summary>Creates a builder for a batch loader.</summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">The service provider required services are resolved from.</param>
    public static BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> Create(string pipelineName,
        IServiceProvider serviceProvider) => new(pipelineName, serviceProvider, routeEventType: null);

    /// <summary>Creates a builder for one route of a loader that routes several CloudEvent types; the
    /// route's event type scopes its idempotency records (see <see cref="WithIdempotency"/>).</summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="serviceProvider">The service provider required services are resolved from.</param>
    /// <param name="routeEventType">The CloudEvent type the route handles.</param>
    public static BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> Create(string pipelineName,
        IServiceProvider serviceProvider, string routeEventType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routeEventType);
        return new BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx>(pipelineName, serviceProvider, routeEventType);
    }

    /// <summary>Configures the per-entry deserializer (CloudEvent data → <typeparamref name="TInput"/>).</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithDeserializer(DeserializeStep<TInput, TCtx> deserializer)
    {
        _deserializer = deserializer;
        return this;
    }

    /// <summary>Keeps only the entries <paramref name="predicate"/> accepts; the others are consumed
    /// before any lookup or send.</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> Where(Func<TInput, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _filter = predicate;
        return this;
    }

    /// <summary>
    /// Configures the entity key: entries with the same key in one batch run once, as their latest
    /// event (by CloudEvent time), and share its result. It is also the lookup key and the
    /// idempotency id. Use the event id when events are not about a shared entity.
    /// </summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> KeyedBy(Func<TInput, string> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keySelector = keySelector;
        return this;
    }

    /// <summary>Looks up the entities' current state per chunk of distinct keys; the per-entity steps
    /// run on it.</summary>
    /// <param name="lookup">The lookup.</param>
    /// <param name="chunkSize">The most keys per lookup call — the source system's limit, independent
    /// of how many entries a delivered batch holds.</param>
    /// <param name="missing">What to do with an entry whose key the lookup did not return.</param>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithLookup(BatchLookupStep<TEnriched> lookup,
        int chunkSize = 100, MissingItemPolicy missing = MissingItemPolicy.Retry)
    {
        ArgumentNullException.ThrowIfNull(lookup);
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 1);
        _lookup = lookup;
        _chunkSize = chunkSize;
        _missing = missing;
        return this;
    }

    /// <summary>Configures the per-entity validator.</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithValidator(ValidateStep<TEnriched, TCtx> validator)
    {
        _validator = validator;
        return this;
    }

    /// <summary>Configures the per-entity transformer.</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithTransformer(
        TransformStep<TEnriched, TOutput, TCtx> transformer)
    {
        _transformer = transformer;
        return this;
    }

    /// <summary>Configures the per-entity sender.</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithSender(SendStep<TOutput, TCtx> sender)
    {
        _sender = sender;
        return this;
    }

    /// <summary>Resolves the registered <see cref="SendStep{T,TCtx}"/> as the per-entity sender.</summary>
    /// <exception cref="InvalidOperationException">No sender is registered.</exception>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithSenderFromServices()
    {
        _sender = _serviceProvider.GetService<SendStep<TOutput, TCtx>>()
                  ?? throw new InvalidOperationException(
                      $"SendStep<{typeof(TOutput).Name}, {typeof(TCtx).Name}> is not registered in the service provider.");
        return this;
    }

    /// <summary>How many entities run through the per-entity steps at the same time (default 1:
    /// one after another, in delivery order).</summary>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithMaxConcurrency(int maxConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        _maxConcurrency = maxConcurrency;
        return this;
    }

    /// <summary>
    /// Configures idempotency on the per-entity state: the id is the entity key (see
    /// <see cref="KeyedBy"/>), the date the CloudEvent time, and the hash is of
    /// <typeparamref name="TEnriched"/> — so with a lookup, an event that changed nothing the
    /// destination sees is recognised as a duplicate.
    /// </summary>
    /// <param name="hashGenerator">Optional hash of the state. Defaults to property reflection + SHA256.</param>
    /// <param name="scope">In a routing loader: the route's own records (the id is
    /// <c>{event type}:{key}</c>, the default) or records shared across routes (the key alone).</param>
    /// <exception cref="InvalidOperationException">No <see cref="IIdempotencyServiceClient"/> is registered.</exception>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithIdempotency(
        Func<TEnriched, string>? hashGenerator = null, IdempotencyScope scope = IdempotencyScope.Route)
    {
        var client = _serviceProvider.GetService<IIdempotencyServiceClient>()
                     ?? throw new InvalidOperationException(
                         $"{nameof(IIdempotencyServiceClient)} is not registered in the service provider.");
        var idPrefix = scope == IdempotencyScope.Route && _routeEventType is not null ? $"{_routeEventType}:" : "";

        _idempotencyChecker = new ExternalIdempotencyChecker<TEnriched, TCtx>(client, _frameworkOptions,
            (_, context) => idPrefix + context.Metadata[BatchContextKeys.EntityKey], (_, context) => EventTime(context),
            hashGenerator);
        _idempotencyRecorder = new ExternalIdempotencyRecorder<TOutput, TCtx>(client, _frameworkOptions);
        return this;
    }

    /// <summary>Configures business incident routing.</summary>
    /// <param name="messageIdExtractor">The incident's message id: stable across retries of the entity's event.</param>
    /// <param name="subjectExtractor">The incident's subject: an id a process owner recognises.</param>
    /// <exception cref="InvalidOperationException">No <see cref="IBusinessIncidentServiceClient"/> is registered.</exception>
    public BatchLoaderBuilder<TInput, TEnriched, TOutput, TCtx> WithBusinessIncidents(
        Func<TCtx, string> messageIdExtractor, Func<TCtx, string> subjectExtractor)
    {
        var client = _serviceProvider.GetService<IBusinessIncidentServiceClient>()
                     ?? throw new InvalidOperationException(
                         $"{nameof(IBusinessIncidentServiceClient)} is not registered in the service provider.");
        _businessIncidentRouter = new ExternalBusinessIncidentRouter<TOutput, TCtx>(client, _frameworkOptions,
            messageIdExtractor, subjectExtractor, defaultValueFactory: () => default!);
        return this;
    }

    /// <summary>Builds the batch loader.</summary>
    /// <exception cref="InvalidOperationException">A required step is missing, or there is no lookup
    /// while <typeparamref name="TEnriched"/> is not <typeparamref name="TInput"/>.</exception>
    public BatchLoader<TInput, TEnriched, TOutput, TCtx> Build()
    {
        if (_deserializer is null)
            throw new InvalidOperationException("Deserialize step must be configured using WithDeserializer()");
        if (_keySelector is null)
            throw new InvalidOperationException("The entity key must be configured using KeyedBy()");
        if (_lookup is null && typeof(TEnriched) != typeof(TInput))
            throw new InvalidOperationException(
                $"Without a lookup the per-entity steps run on the event itself, so {typeof(TEnriched).Name} must be " +
                $"{typeof(TInput).Name}; configure one using WithLookup()");
        if (_validator is null)
            throw new InvalidOperationException("Validate step must be configured using WithValidator()");
        if (_idempotencyChecker is null || _idempotencyRecorder is null)
            throw new InvalidOperationException("Idempotency must be configured using WithIdempotency()");
        if (_transformer is null)
            throw new InvalidOperationException("Transform step must be configured using WithTransformer()");
        if (_sender is null)
            throw new InvalidOperationException("Sender step must be configured using WithSender() or WithSenderFromServices()");
        if (_businessIncidentRouter is null)
            throw new InvalidOperationException("Business incident route step must be configured using WithBusinessIncidents()");

        return new BatchLoader<TInput, TEnriched, TOutput, TCtx>(_pipelineName, _logger, _deserializer, _filter,
            _keySelector, _lookup, _chunkSize, _missing, _validator, _idempotencyChecker, _transformer, _sender,
            _idempotencyRecorder, _businessIncidentRouter, _maxConcurrency);
    }

    private static DateTimeOffset EventTime(TCtx context)
    {
        if (!context.Metadata.TryGetValue(CloudEventContextKeys.Time, out var time) || string.IsNullOrEmpty(time))
            throw new InvalidOperationException(
                $"Missing required CloudEvent.Time in context (key: '{CloudEventContextKeys.Time}'). Ensure the CloudEvent has a Time set.");
        if (!DateTimeOffset.TryParse(time, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date))
            throw new InvalidOperationException($"Invalid RFC 3339 date format in CloudEvent.Time: '{time}'");
        return date;
    }
}
