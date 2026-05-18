using Intropy.Framework.Core.Pipeline.Abstractions.Enums;
using Intropy.Framework.Core.Pipeline.Abstractions.Steps;

namespace Intropy.Framework.Blocks.Shared.Steps;

/// <summary>
/// An abstract base for routing business incidents.
/// </summary>
/// <typeparam name="T">The type of the return value from the previous step</typeparam>
/// <typeparam name="TCtx">The type of the context</typeparam>
public abstract class BusinessIncidentRouteStep<T, TCtx> : Finalizer<T, TCtx>
{
    /// <inheritdoc />
    public override string FinalizerName => "RouteBusinessIncidents";

    /// <inheritdoc />
    public override FinalizerTrigger Triggers => FinalizerTrigger.OnBusinessFailure | FinalizerTrigger.OnSuccess;
}