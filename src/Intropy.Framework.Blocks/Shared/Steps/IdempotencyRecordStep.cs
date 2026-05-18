using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Shared.Steps;

/// <summary>
/// An abstract base for recording idempotency.
/// </summary>
/// <typeparam name="T">The type of the return value from the previous step</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class IdempotencyRecordStep<T, TCtx> : TechnicalStep<T, T, TCtx>
{
    /// <inheritdoc />
    public override string StepName => "IdempotencyRecord";
}