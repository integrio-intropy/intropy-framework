using Intropy.Framework.Blocks.Extractor;
using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Components;

/// <summary>
/// One route of a component that publishes what it takes in: the steps its pipeline runs and the
/// rules it needs that only the component knows. The steps are the classes the route names —
/// <see cref="DeserializeStep{T,TCtx}"/> and <see cref="ValidateStep{T,TCtx}"/> for
/// <typeparamref name="TInput"/>, <see cref="TransformStep{TInput,TOutput,TCtx}"/> to
/// <typeparamref name="TOutput"/>; the framework wires the publisher, the idempotency check and the
/// incident routing around them.
/// </summary>
/// <remarks>
/// <code>
/// route =&gt; route
///     .Steps&lt;OrderDeserializer, OrderValidator, OrderTransformer&gt;()
///     .Idempotency(key: (order, _) =&gt; order.OrderId, at: (order, _) =&gt; order.OrderDate)
///     .OutputEvent(subject: order =&gt; order.OrderId, at: order =&gt; order.OrderDate)
/// </code>
/// </remarks>
/// <typeparam name="TInput">The route's deserialized input.</typeparam>
/// <typeparam name="TOutput">The published payload: the projection of the component's message.</typeparam>
public sealed class PublishingRoute<TInput, TOutput>
{
    private (Type Deserializer, Type Validator, Type Transformer)? _steps;
    private (Func<TInput, Context, string> Key, Func<TInput, Context, DateTimeOffset> At, IdempotencyScope Scope)? _idempotency;
    private (Func<TOutput, string> Subject, Func<TOutput, DateTimeOffset> At)? _outputEvent;
    private IncidentRule? _incidents;
    private Func<ExtractorBuilder<TInput, TOutput, Context>, IServiceProvider, ExtractorBuilder<TInput, TOutput, Context>>? _extend;

    internal PublishingRoute()
    {
    }

    /// <summary>The declared steps, or null when <see cref="Steps{TDeserializer,TValidator,TTransformer}"/>
    /// has not been called.</summary>
    internal (Type Deserializer, Type Validator, Type Transformer)? DeclaredSteps => _steps;

    /// <summary>Names the pipeline's steps: the class that deserializes the route's input, the one
    /// that validates it, and the one that transforms it to the published payload. Each is bound to
    /// its slot — its step base type for <typeparamref name="TInput"/> and
    /// <typeparamref name="TOutput"/>. Required.</summary>
    /// <typeparam name="TDeserializer">The deserializer, deriving from
    /// <see cref="DeserializeStep{T,TCtx}"/>.</typeparam>
    /// <typeparam name="TValidator">The validator, deriving from <see cref="ValidateStep{T,TCtx}"/>.</typeparam>
    /// <typeparam name="TTransformer">The transformer, deriving from
    /// <see cref="TransformStep{TInput,TOutput,TCtx}"/>.</typeparam>
    /// <returns>The route, for chaining.</returns>
    /// <remarks>
    /// The classes resolve from DI in the item's own scope, so a constructor may take services; each
    /// is registered as a transient service of itself, unless the component already registered that
    /// class. A component that registers a slot — the step base type — in
    /// <c>ConfigureServices</c> overrides the route's declared class: an author-registered slot
    /// wins. Two endpoints accepting the same input type may name the same classes, or each its own.
    /// </remarks>
    public PublishingRoute<TInput, TOutput> Steps<TDeserializer, TValidator, TTransformer>()
        where TDeserializer : DeserializeStep<TInput, Context>
        where TValidator : ValidateStep<TInput, Context>
        where TTransformer : TransformStep<TInput, TOutput, Context>
    {
        _steps = (typeof(TDeserializer), typeof(TValidator), typeof(TTransformer));
        return this;
    }

    /// <summary>How the idempotency check recognizes a record it has seen: a stable business identity
    /// and the source version. Required — only the component knows its records' identity; per-delivery
    /// values (a fresh GUID, the wall-clock time) would let every duplicate through.</summary>
    /// <param name="key">The record's business identity.</param>
    /// <param name="at">The record's version in the source system.</param>
    /// <param name="scope">Whether the route's ports keep their own records (the default) or share
    /// them with every port of the component.</param>
    /// <returns>The route, for chaining.</returns>
    public PublishingRoute<TInput, TOutput> Idempotency(Func<TInput, Context, string> key,
        Func<TInput, Context, DateTimeOffset> at, IdempotencyScope scope = IdempotencyScope.Route)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(at);
        _idempotency = (key, at, scope);
        return this;
    }

    /// <summary>The envelope members the payload decides: what the event is about and when it
    /// happened in the source system. Required. The framework sets the rest — the type (the message),
    /// the source (the component's <see cref="IntropyComponentAttribute"/>), the id and the data.</summary>
    /// <param name="subject">The entity the event is about.</param>
    /// <param name="at">When the event occurred in the source system.</param>
    /// <returns>The route, for chaining.</returns>
    public PublishingRoute<TInput, TOutput> OutputEvent(Func<TOutput, string> subject, Func<TOutput, DateTimeOffset> at)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(at);
        _outputEvent = (subject, at);
        return this;
    }

    /// <summary>How a business incident names its item. Default: the block's — for a gateway,
    /// <see cref="IncidentRule.ByBusinessIdOrRequest"/>.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The route, for chaining.</returns>
    public PublishingRoute<TInput, TOutput> Incidents(IncidentRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        _incidents = rule;
        return this;
    }

    /// <summary>Extends the framework's pipeline after it is configured — e.g.
    /// <c>(builder, services) =&gt; builder.WithExtractor(new Enrich())</c> — or replaces a part of it.</summary>
    /// <param name="extend">Adds to the configured builder, given the item's scope.</param>
    /// <returns>The route, for chaining.</returns>
    public PublishingRoute<TInput, TOutput> Extend(
        Func<ExtractorBuilder<TInput, TOutput, Context>, IServiceProvider, ExtractorBuilder<TInput, TOutput, Context>> extend)
    {
        ArgumentNullException.ThrowIfNull(extend);
        _extend = extend;
        return this;
    }

    /// <summary>The route's pipeline configurator: the framework's chain over the route's steps and
    /// rules, then the route's extension.</summary>
    /// <param name="route">The route, as errors name it.</param>
    /// <param name="defaultIncidents">The block's incident rule, for a route that sets none.</param>
    /// <exception cref="InvalidOperationException">A required rule is missing.</exception>
    internal Func<ExtractorBuilder<TInput, TOutput, Context>, IServiceProvider, ExtractorBuilder<TInput, TOutput, Context>>
        Pipeline(string route, IncidentRule defaultIncidents)
    {
        // The steps were named when the route registered them; every caller composes after that.
        var steps = _steps!.Value;
        var idempotency = _idempotency ?? throw new InvalidOperationException(
            $"{route} has no idempotency rule: call Idempotency(key, at) on the route.");
        var outputEvent = _outputEvent ?? throw new InvalidOperationException(
            $"{route} has no output event rule: call OutputEvent(subject, at) on the route.");
        var incidents = _incidents ?? defaultIncidents;
        var extend = _extend;
        return (builder, services) =>
        {
            builder
                .WithDeserializer(Step<DeserializeStep<TInput, Context>>(services, steps.Deserializer))
                .WithValidator(Step<ValidateStep<TInput, Context>>(services, steps.Validator))
                .WithIdempotency(idempotency.Key, idempotency.At, scope: idempotency.Scope)
                .WithTransformer(Step<TransformStep<TInput, TOutput, Context>>(services, steps.Transformer))
                .WithSerializer(new CloudEventSerializeStep<TOutput, Context>(outputEvent.Subject, outputEvent.At))
                .WithSenderFromServices()
                .WithBusinessIncidents(incidents.Identity, incidents.Subject);
            return extend is null ? builder : extend(builder, services);
        };
    }

    /// <summary>Resolves a step in the item's scope: the slot, when the component registered one in
    /// <c>ConfigureServices</c> — an author-registered slot wins — else the route's declared class.</summary>
    private static TSlot Step<TSlot>(IServiceProvider services, Type declared) where TSlot : class =>
        services.GetService<TSlot>() ?? (TSlot)services.GetRequiredService(declared);
}
