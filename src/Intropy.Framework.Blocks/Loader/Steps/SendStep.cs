using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Loader.Steps;

/// <summary>
/// A step that sends data to an external system.
/// </summary>
/// <typeparam name="T">The type of data to send</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class SendStep<T, TCtx> : TechnicalStep<T, T, TCtx> where TCtx : Context
{
    /// <inheritdoc/>
    public override string StepName => "Send";
}
