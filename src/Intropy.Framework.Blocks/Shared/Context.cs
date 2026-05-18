namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// Base class for representing the context in a pipeline.
/// </summary>
/// <param name="Metadata">A dictionary to store key value metadata.</param>
/// <param name="IsRetry">A boolean that represents if the message being processed is retrying.</param>
public record Context(
    Dictionary<string, string> Metadata,
    bool IsRetry = false
);
