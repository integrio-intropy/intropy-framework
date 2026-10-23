using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components.Gateway;

/// <summary>
/// The pipeline one gateway endpoint runs its items through, as the component declares it: the
/// steps and the rules only the component knows. The steps are the classes the component writes,
/// named by their role in the order they run (<see cref="Steps"/>); the rules configure the steps
/// the framework owns — the idempotency check, the serializer and the incident routing. The
/// framework turns the declaration into the extractor pipeline, with the publisher around the
/// steps.
/// </summary>
/// <remarks>
/// <code>
/// pipeline =&gt; pipeline
///     .Steps(steps =&gt; steps
///         .Deserialize&lt;OrderDeserializer&gt;()
///         .Validate&lt;OrderValidator&gt;()
///         .Transform&lt;OrderTransformer&gt;())
///     .Idempotency(key: (order, _) =&gt; order.OrderId, at: (order, _) =&gt; order.OrderDate)
///     .OutputEvent(subject: order =&gt; order.OrderId, at: order =&gt; order.OrderDate)
/// </code>
/// </remarks>
/// <typeparam name="TInput">The endpoint's deserialized request body.</typeparam>
/// <typeparam name="TPayload">The published payload: the projection of the component's message.</typeparam>
/// <typeparam name="TCtx">The pipeline\'s context type.</typeparam>
public sealed class GatewayPipelineDeclaration<TInput, TPayload, TCtx>
    where TCtx : Context
{
    private DeclaredGatewaySteps<TInput, TPayload, TCtx>? _steps;
    private (Func<TInput, TCtx, string> Key, Func<TInput, TCtx, DateTimeOffset> At, IdempotencyScope Scope)? _idempotency;
    private bool _noIdempotency;
    private (Func<TPayload, string> Subject, Func<TPayload, DateTimeOffset> At)? _outputEvent;
    private IncidentRule? _incidents;

    internal GatewayPipelineDeclaration()
    {
    }

    /// <summary>The declared steps, or null when <see cref="Steps"/> has not been called.</summary>
    internal DeclaredGatewaySteps<TInput, TPayload, TCtx>? DeclaredSteps => _steps;

    /// <summary>Names the pipeline's steps, by their role, in the order they run: the class that
    /// deserializes the endpoint's input, the one that validates it, any that enrich it, and the one
    /// that transforms it to the published payload. Required.</summary>
    /// <param name="declare">Names the steps: <c>Deserialize</c>, <c>Validate</c>, any number of
    /// <c>Extract</c>, then <c>Transform</c> — each call offers only the steps that may come
    /// next.</param>
    /// <returns>The declaration, for chaining.</returns>
    /// <remarks>
    /// The classes resolve from DI in the item's own scope, so a constructor may take services; each
    /// is registered as a transient service of itself, unless the component already registered that
    /// class. Two endpoints accepting the same input type may name the same classes, or each its own.
    /// </remarks>
    public GatewayPipelineDeclaration<TInput, TPayload, TCtx> Steps(
        Func<GatewaySteps<TInput, TPayload, TCtx>, DeclaredGatewaySteps<TInput, TPayload, TCtx>> declare)
    {
        ArgumentNullException.ThrowIfNull(declare);
        _steps = declare(new GatewaySteps<TInput, TPayload, TCtx>())
                 ?? throw new InvalidOperationException("Steps(...) must return the declared steps, ending with Transform<T>().");
        return this;
    }

    /// <summary>How the idempotency check recognizes a record it has seen: a stable business identity
    /// and the source version. Required, unless the pipeline declares <see cref="NoIdempotency"/> —
    /// only the component knows its records' identity; per-delivery values (a fresh GUID, the
    /// wall-clock time) would let every duplicate through.</summary>
    /// <param name="key">The record's business identity.</param>
    /// <param name="at">The record's version in the source system.</param>
    /// <param name="scope">Whether the endpoint's port keeps its own records (the default) or shares
    /// them with every port of the component.</param>
    /// <returns>The declaration, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The pipeline already declares
    /// <see cref="NoIdempotency"/>.</exception>
    public GatewayPipelineDeclaration<TInput, TPayload, TCtx> Idempotency(Func<TInput, TCtx, string> key,
        Func<TInput, TCtx, DateTimeOffset> at, IdempotencyScope scope = IdempotencyScope.Route)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(at);
        if (_noIdempotency)
            throw new InvalidOperationException(
                "The pipeline declares NoIdempotency(): declare either an idempotency rule or none, not both.");
        _idempotency = (key, at, scope);
        return this;
    }

    /// <summary>Declares that the pipeline runs no idempotency check: every accepted item is published,
    /// and the Idempotency Service is never called. The explicit alternative to
    /// <see cref="Idempotency"/> — a pipeline that declares neither fails at startup.</summary>
    /// <remarks>
    /// The caller is the gateway's retry mechanism, so a resend — after a timeout, say — publishes the
    /// item again, and the gateway never answers <c>200</c> for a duplicate. Declare it only when the
    /// consumers deduplicate (a loader's idempotency check) or duplicates are harmless, and leave the
    /// component's <c>Calls(Services.Idempotency)</c> out of the system's topology.
    /// </remarks>
    /// <returns>The declaration, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The pipeline already declares an idempotency
    /// rule.</exception>
    public GatewayPipelineDeclaration<TInput, TPayload, TCtx> NoIdempotency()
    {
        if (_idempotency is not null)
            throw new InvalidOperationException(
                "The pipeline declares an idempotency rule: declare either an idempotency rule or none, not both.");
        _noIdempotency = true;
        return this;
    }

    /// <summary>The envelope members the payload decides: what the event is about and when it
    /// happened in the source system. Required. The framework sets the rest — the type (the message),
    /// the source (the component's <see cref="ComponentManifest.EventSource"/>), the id and the data.</summary>
    /// <param name="subject">The entity the event is about.</param>
    /// <param name="at">When the event occurred in the source system.</param>
    /// <returns>The declaration, for chaining.</returns>
    public GatewayPipelineDeclaration<TInput, TPayload, TCtx> OutputEvent(Func<TPayload, string> subject, Func<TPayload, DateTimeOffset> at)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(at);
        _outputEvent = (subject, at);
        return this;
    }

    /// <summary>How a business incident names its item. Default: the block's — for a gateway,
    /// <see cref="IncidentRule.ByBusinessIdOrRequest"/>.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The declaration, for chaining.</returns>
    public GatewayPipelineDeclaration<TInput, TPayload, TCtx> Incidents(IncidentRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        _incidents = rule;
        return this;
    }

    /// <summary>The extractor pipeline's configurator: the framework's chain over the declared steps
    /// and rules.</summary>
    /// <param name="name">The pipeline, as errors name it.</param>
    /// <param name="defaultIncidents">The block's incident rule, for a declaration that sets none.</param>
    /// <exception cref="InvalidOperationException">A required rule is missing.</exception>
    internal Func<ExtractorBuilder<TInput, TPayload, TCtx>, IServiceProvider, ExtractorBuilder<TInput, TPayload, TCtx>>
        Pipeline(string name, IncidentRule defaultIncidents)
    {
        // The steps were checked when the endpoint registered them; every caller composes after that.
        var steps = _steps!;
        if (_idempotency is null && !_noIdempotency)
            throw new InvalidOperationException(
                $"{name} has no idempotency rule: call Idempotency(key, at) on the pipeline, or NoIdempotency() " +
                "to publish every item, duplicates included.");
        var idempotency = _idempotency;
        var outputEvent = _outputEvent ?? throw new InvalidOperationException(
            $"{name} has no output event rule: call OutputEvent(subject, at) on the pipeline.");
        var incidents = _incidents ?? defaultIncidents;
        return (builder, services) =>
        {
            builder
                .WithDeserializer((DeserializeStep<TInput, TCtx>)services.GetRequiredService(steps.Deserializer))
                .WithValidator((ValidateStep<TInput, TCtx>)services.GetRequiredService(steps.Validator));
            foreach (var extractor in steps.Extractors)
                builder.WithExtractor((ExtractStep<TInput, TCtx>)services.GetRequiredService(extractor));
            // Without a rule the extractor leaves out its check and record steps.
            if (idempotency is { } rule)
                builder.WithIdempotency(rule.Key, rule.At, scope: rule.Scope);
            return builder
                .WithTransformer((TransformStep<TInput, TPayload, TCtx>)services.GetRequiredService(steps.Transformer))
                .WithSerializer(new CloudEventSerializeStep<TPayload, TCtx>(outputEvent.Subject, outputEvent.At))
                .WithSenderFromServices()
                .WithBusinessIncidents(incidents.Identity, incidents.Subject);
        };
    }
}
