using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;

/// <summary>
/// Reads content from a source. Override to implement custom source reading logic.
/// Returns BusinessFailure for: missing item, access denied, corrupt data.
/// </summary>
/// <typeparam name="TCtx">The type of the context used in the step.</typeparam>
public abstract class ReceiveStep<TCtx> : BusinessStep<SourceItemInfo, SourceItem, TCtx>
    where TCtx : Context
{
    /// <inheritdoc />
    public override string StepName => "Receive";
}
