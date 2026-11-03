namespace Intropy.Framework.Blocks.Shared;

/// <summary>
/// The keys the archive sets: on the pipeline context, and on the context of a business incident
/// whose payload it archived.
/// </summary>
public static class ArchiveContextKeys
{
    /// <summary>
    /// The archive key of the item's payload, on the pipeline context, once it has been written. An
    /// incident raised after it reuses the copy instead of writing a second one. Local to the
    /// pipeline: it is not propagated over a queue or a topic, where the next component archives
    /// what it receives itself.
    /// </summary>
    public const string Ref = "archive_ref";

    /// <summary>The archive key of the payload, on a business incident's context.</summary>
    public const string IncidentDataRef = "dataref";

    /// <summary>Where the archived payload can be opened, on a business incident's context. Only
    /// set when the component configures a browse base URL.</summary>
    public const string IncidentDataUrl = "dataurl";
}
