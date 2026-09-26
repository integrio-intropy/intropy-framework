namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Creates the context for one item a host runs through a pipeline (a source file, or a message
/// from the queue). The parameters mirror <see cref="Context"/>'s own, so a derived context is
/// usually <c>(metadata, isRetry) =&gt; new OrderContext(metadata, isRetry)</c>.
/// </summary>
/// <remarks>
/// The host creates a fresh <paramref name="metadata"/> dictionary per item, already holding the
/// keys it owns (such as <see cref="SourceContextKeys.FileName"/>, or the metadata propagated
/// with a message), and calls the factory once per item. Return a new context each time;
/// construction must not use reflection or assume a parameterless constructor.
/// </remarks>
/// <typeparam name="TCtx">The pipeline's context type.</typeparam>
/// <param name="metadata">The item's metadata, owned by the new context.</param>
/// <param name="isRetry">Whether the item is being redelivered (always false for source files).</param>
public delegate TCtx ContextFactory<out TCtx>(Dictionary<string, string> metadata, bool isRetry) where TCtx : Context;
