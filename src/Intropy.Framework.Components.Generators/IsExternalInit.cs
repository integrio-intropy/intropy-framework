// A source generator's record types need init-only setters; netstandard2.0 ships no
// IsExternalInit, so the build must define it. Internal: it is the build's, not the generator's
// users'.
namespace System.Runtime.CompilerServices;

internal static class IsExternalInit
{
}
