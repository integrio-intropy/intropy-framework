using Intropy.Framework.Blocks.Shared;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// Marks one component kind as registered on a provider. The marker is internal and carries the
/// component kind, so the one-component-per-provider check across the registration entry points
/// lives in one place (<see cref="EnsureNoOther"/>) with one message format.
/// </summary>
internal static class ComponentRegistration
{
    private const string CallbackPortMember = "CallbackPort";

    /// <summary>Throws if a <paramref name="componentKind"/> component is already registered on
    /// the provider. The read-only half of the guard: call it before validation, then
    /// <see cref="MarkRegistered"/> once the registration can no longer fail.</summary>
    /// <param name="services">The service collection being registered on.</param>
    /// <param name="componentKind">The component kind, in the singular ("extractor", "loader").</param>
    /// <exception cref="InvalidOperationException">A <paramref name="componentKind"/> component is
    /// already registered on this provider.</exception>
    internal static void EnsureSingleKind(IServiceCollection services, string componentKind)
    {
        if (services.Any(d => d.ServiceType == typeof(ComponentMarker) &&
                d.ImplementationInstance is ComponentMarker marker &&
                marker.ComponentKind == componentKind))
            throw new InvalidOperationException(
                $"Only one {componentKind} component may be registered per service provider.");
    }

    /// <summary>Records that a <paramref name="componentKind"/> component is being registered, so
    /// the next <see cref="EnsureSingleKind"/> sees it and rejects the second one: the decision a
    /// component makes once — "which extractor, which loader" — cannot be made twice on one
    /// provider.</summary>
    internal static void MarkRegistered(IServiceCollection services, string componentKind) =>
        services.AddSingleton(new ComponentMarker(componentKind));

    /// <summary>Checks that no <paramref name="componentKind"/> component is registered yet and
    /// marks this one, the guard in one call for entry points whose only earlier failure mode is a
    /// null argument. For entry points that validate configuration after the check, use
    /// <see cref="EnsureSingleKind"/> followed by <see cref="MarkRegistered"/> — a rejected
    /// registration must leave no marker behind, or a corrected retry looks like a duplicate.</summary>
    /// <param name="services">The service collection being registered on.</param>
    /// <param name="componentKind">The component kind, in the singular ("extractor", "loader").</param>
    /// <exception cref="InvalidOperationException">A <paramref name="componentKind"/> component is
    /// already registered on this provider.</exception>
    internal static void EnsureNoOther(IServiceCollection services, string componentKind)
    {
        EnsureSingleKind(services, componentKind);
        MarkRegistered(services, componentKind);
    }

    /// <summary>Checks the port a component serves the Dapr sidecar's gRPC app callback on.
    /// A wrong port surfaces here, at registration, instead of as a sidecar push failure.</summary>
    /// <param name="callbackPort">The configured port, or null for the <c>APP_PORT</c> default.</param>
    /// <param name="optionsName">The options type the port was set through, named in the error.</param>
    /// <param name="paramName">The registration parameter being checked, for the exception.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="callbackPort"/> is outside
    /// 1–65535.</exception>
    internal static void EnsureValidCallbackPort(int? callbackPort, string optionsName, string paramName)
    {
        if (callbackPort is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(paramName, callbackPort,
                $"{optionsName}.{CallbackPortMember} must be a port number (1-65535).");
    }

    /// <summary>The context factory for components on the base <see cref="Context"/>: the plain
    /// record, no derived state, so callers need not restate the obvious.</summary>
    /// <param name="componentKind">The component kind, for the error.</param>
    /// <param name="memberName">The definition member the factory would have been set through,
    /// named in the error.</param>
    /// <typeparam name="TCtx">The pipeline context.</typeparam>
    /// <returns>The default factory when <typeparamref name="TCtx"/> is <see cref="Context"/>.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="TCtx"/> derives from
    /// <see cref="Context"/> and has no default factory.</exception>
    internal static ContextFactory<TCtx> DefaultContextFactory<TCtx>(string componentKind, string memberName)
        where TCtx : Context
    {
        if (typeof(TCtx) == typeof(Context))
            return static (metadata, isRetry) => (TCtx)(object)new Context(metadata, isRetry);

        throw new InvalidOperationException(
            $"{componentKind} composition failed: {memberName} — {typeof(TCtx).Name} derives from Context " +
            $"and needs its own context factory; set {memberName} to (metadata, isRetry) => new {typeof(TCtx).Name}(metadata, isRetry).");
    }

    private sealed record ComponentMarker(string ComponentKind);
}
