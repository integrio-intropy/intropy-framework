using System.Diagnostics;

namespace Intropy.Framework.Adapters.Common;

internal static class ActivitySourceProvider
{
    private const string ActivitySourceName = "Intropy.Framework.Adapters";
    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
