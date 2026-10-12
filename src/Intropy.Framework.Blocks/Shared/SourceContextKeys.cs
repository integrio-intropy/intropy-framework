namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Dictionary keys the framework guarantees on the pipeline context for source-driven
/// components (e.g. a file sweep feeding an extractor pipeline).
/// </summary>
public static class SourceContextKeys
{
    /// <summary>
    /// The source file name (item identity) of the item being processed. The framework
    /// sets this before any step runs — including deserialization — so business-incident
    /// routing can identify a poisoned source file.
    /// </summary>
    public const string FileName = "file_name";

    /// <summary>
    /// The source port the item being processed was swept from, as declared in the system's
    /// topology. The framework sets this next to <see cref="FileName"/>, before any step runs, so
    /// steps, incident routing and idempotency can tell a component's sources apart.
    /// </summary>
    public const string SourcePort = "source_port";
}
