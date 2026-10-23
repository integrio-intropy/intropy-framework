using Intropy.Framework.Blocks.Extractor.Steps;
using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Components.Gateway;

/// <summary>
/// The steps of one gateway pipeline, being declared: the classes the component writes, named by
/// their role, in the order the pipeline runs them. Each call offers only the steps that may come
/// next, so a declaration that compiles is complete and in order:
/// </summary>
/// <remarks>
/// <code>
/// steps =&gt; steps
///     .Deserialize&lt;OrderDeserializer&gt;()
///     .Validate&lt;OrderValidator&gt;()
///     .Extract&lt;CustomerLookup&gt;()      // optional, repeatable, run in order
///     .Transform&lt;OrderTransformer&gt;()
/// </code>
/// The steps the framework owns — the idempotency check and record, the serializer, the publisher,
/// the incident routing — are not declared here; the pipeline's rules configure them.
/// </remarks>
/// <typeparam name="TInput">The endpoint's deserialized request body.</typeparam>
/// <typeparam name="TPayload">The published payload: the projection of the component's message.</typeparam>
/// <typeparam name="TCtx">The pipeline\'s context type.</typeparam>
public sealed class GatewaySteps<TInput, TPayload, TCtx>
    where TCtx : Context
{
    internal GatewaySteps()
    {
    }

    /// <summary>The step that deserializes the request body into <typeparamref name="TInput"/>.</summary>
    /// <typeparam name="TDeserializer">The deserializer, deriving from
    /// <see cref="DeserializeStep{T,TCtx}"/>.</typeparam>
    /// <returns>The steps, to name the validator.</returns>
    public DeserializedGatewaySteps<TInput, TPayload, TCtx> Deserialize<TDeserializer>()
        where TDeserializer : DeserializeStep<TInput, TCtx> =>
        new(typeof(TDeserializer));
}

/// <summary>A gateway pipeline's steps after the deserializer: the validator comes next.</summary>
/// <typeparam name="TInput">The endpoint's deserialized request body.</typeparam>
/// <typeparam name="TPayload">The published payload.</typeparam>
/// <typeparam name="TCtx">The pipeline\'s context type.</typeparam>
public sealed class DeserializedGatewaySteps<TInput, TPayload, TCtx>
    where TCtx : Context
{
    private readonly Type _deserializer;

    internal DeserializedGatewaySteps(Type deserializer) => _deserializer = deserializer;

    /// <summary>The step that validates the deserialized <typeparamref name="TInput"/>.</summary>
    /// <typeparam name="TValidator">The validator, deriving from <see cref="ValidateStep{T,TCtx}"/>.</typeparam>
    /// <returns>The steps, to name the extract steps, if any, and the transformer.</returns>
    public ValidatedGatewaySteps<TInput, TPayload, TCtx> Validate<TValidator>()
        where TValidator : ValidateStep<TInput, TCtx> =>
        new(_deserializer, typeof(TValidator), []);
}

/// <summary>A gateway pipeline's steps after the validator: any extract steps, then the
/// transformer.</summary>
/// <typeparam name="TInput">The endpoint's deserialized request body.</typeparam>
/// <typeparam name="TPayload">The published payload.</typeparam>
/// <typeparam name="TCtx">The pipeline\'s context type.</typeparam>
public sealed class ValidatedGatewaySteps<TInput, TPayload, TCtx>
    where TCtx : Context
{
    private readonly Type _deserializer;
    private readonly Type _validator;
    private readonly IReadOnlyList<Type> _extractors;

    internal ValidatedGatewaySteps(Type deserializer, Type validator, IReadOnlyList<Type> extractors) =>
        (_deserializer, _validator, _extractors) = (deserializer, validator, extractors);

    /// <summary>A step that enriches the validated <typeparamref name="TInput"/> — with data from
    /// another system, say. Optional; several run in the order declared, after the validator and
    /// before the idempotency check.</summary>
    /// <typeparam name="TExtractor">The extract step, deriving from
    /// <see cref="ExtractStep{TInput,TCtx}"/>.</typeparam>
    /// <returns>The steps, to name further extract steps and the transformer.</returns>
    public ValidatedGatewaySteps<TInput, TPayload, TCtx> Extract<TExtractor>()
        where TExtractor : ExtractStep<TInput, TCtx> =>
        new(_deserializer, _validator, [.. _extractors, typeof(TExtractor)]);

    /// <summary>The step that transforms <typeparamref name="TInput"/> to the published
    /// <typeparamref name="TPayload"/>; the last step the component declares.</summary>
    /// <typeparam name="TTransformer">The transformer, deriving from
    /// <see cref="TransformStep{TInput,TOutput,TCtx}"/>.</typeparam>
    /// <returns>The declared steps.</returns>
    public DeclaredGatewaySteps<TInput, TPayload, TCtx> Transform<TTransformer>()
        where TTransformer : TransformStep<TInput, TPayload, TCtx> =>
        new(_deserializer, _validator, _extractors, typeof(TTransformer));
}

/// <summary>A gateway pipeline's declared steps, complete: what
/// <see cref="GatewayPipelineDeclaration{TInput,TPayload,TCtx}.Steps"/> takes.</summary>
/// <typeparam name="TInput">The endpoint's deserialized request body.</typeparam>
/// <typeparam name="TPayload">The published payload.</typeparam>
/// <typeparam name="TCtx">The pipeline\'s context type.</typeparam>
public sealed class DeclaredGatewaySteps<TInput, TPayload, TCtx>
    where TCtx : Context
{
    internal DeclaredGatewaySteps(Type deserializer, Type validator, IReadOnlyList<Type> extractors, Type transformer) =>
        (Deserializer, Validator, Extractors, Transformer) = (deserializer, validator, extractors, transformer);

    internal Type Deserializer { get; }

    internal Type Validator { get; }

    /// <summary>The extract steps, in the order they run.</summary>
    internal IReadOnlyList<Type> Extractors { get; }

    internal Type Transformer { get; }

    /// <summary>Every step class, in the order the pipeline runs them.</summary>
    internal IEnumerable<Type> All => [Deserializer, Validator, .. Extractors, Transformer];
}
