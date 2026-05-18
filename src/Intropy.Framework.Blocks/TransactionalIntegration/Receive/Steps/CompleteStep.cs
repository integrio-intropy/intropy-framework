using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Handles cleanup after successful publish. Override to implement delete/archive logic.
/// Returns BusinessFailure for: can't delete, can't move to archive.
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
public abstract class CompleteStep<TCtx> : BusinessStep<SourceItem, SourceItem, TCtx>
    where TCtx : Context
{
    /// <inheritdoc />
    public override string StepName => "Complete";
}
