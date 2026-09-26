using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Hosting.Common;

/// <summary>Creates a pipeline context through a component's <see cref="ContextFactory{TCtx}"/>,
/// the same way for every host.</summary>
internal static class ContextCreation
{
    /// <summary>Calls <paramref name="factory"/> and makes sure the host-owned
    /// <paramref name="metadata"/> entries are on the returned context, overwriting caller values.</summary>
    /// <exception cref="InvalidOperationException">The factory returned null or a context
    /// without a metadata dictionary.</exception>
    internal static TCtx Create<TCtx>(ContextFactory<TCtx> factory, Dictionary<string, string> metadata, bool isRetry,
        string componentName, string item) where TCtx : Context
    {
        var context = factory(metadata, isRetry) ?? throw new InvalidOperationException(
            $"Component '{componentName}': the context factory returned null for '{item}'.");
        if (context.Metadata is null)
            throw new InvalidOperationException(
                $"Component '{componentName}': the context returned for '{item}' has no metadata dictionary.");

        if (!ReferenceEquals(context.Metadata, metadata))
            foreach (var (key, value) in metadata)
                context.Metadata[key] = value;
        return context;
    }
}
