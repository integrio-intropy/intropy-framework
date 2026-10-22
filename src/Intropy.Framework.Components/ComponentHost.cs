namespace Intropy.Framework.Components;

/// <summary>
/// The entry point of a component: <c>return await ComponentHost.RunAsync&lt;MyGateway&gt;(args);</c>
/// The component's block decides the host — a gateway serves HTTP until it is stopped.
/// </summary>
public static class ComponentHost
{
    /// <summary>Runs <typeparamref name="TComponent"/> under its block's host and returns the exit
    /// code. Return it from the program.</summary>
    /// <typeparam name="TComponent">The component.</typeparam>
    /// <param name="args">The program's arguments, passed to the host's configuration.</param>
    /// <returns>The exit code.</returns>
    public static Task<int> RunAsync<TComponent>(string[]? args = null) where TComponent : IntropyComponent, new() =>
        new TComponent().RunAsync(args ?? [], ComponentManifest.From(typeof(TComponent)));
}
