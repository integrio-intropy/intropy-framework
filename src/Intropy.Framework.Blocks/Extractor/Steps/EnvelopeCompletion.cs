using CloudNative.CloudEvents;

namespace Intropy.Framework.Blocks.Extractor.Steps;

/// <summary>
/// Fills in the envelope attributes a send step owns from its configuration: the source, and the
/// type when the serialize step did not resolve one.
/// </summary>
/// <remarks>
/// <para>
/// It exists because these two attributes are <em>required</em> — <c>EncodeStructuredModeMessage</c>
/// refuses an envelope missing either — while the serialize step cannot supply the source at all and
/// need not supply the type. Anything that has to encode or measure an envelope before the send step
/// runs, as the claim check does, must therefore complete it first.
/// </para>
/// <para>
/// One copy of the rule, called from every place that applies it, is what keeps the envelope a
/// claim check measures identical to the one the publisher goes on to encode. Applying it twice is
/// harmless: it is idempotent, and the second call assigns the same values the first did.
/// </para>
/// </remarks>
internal static class EnvelopeCompletion
{
    /// <summary>Sets <paramref name="envelope"/>'s source, and its type if it has none.</summary>
    /// <param name="envelope">The envelope being sent.</param>
    /// <param name="source">The configured source: where the data came from. Always the send step's,
    /// never the serialize step's.</param>
    /// <param name="type">The configured type, applied only as a fallback — a type a
    /// component-supplied type extractor already resolved wins.</param>
    internal static void Apply(CloudEvent envelope, Uri source, string type)
    {
        envelope.Source = source;
        envelope.Type ??= type;
    }
}
