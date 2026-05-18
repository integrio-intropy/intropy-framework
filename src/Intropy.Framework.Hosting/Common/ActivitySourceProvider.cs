using System.Diagnostics;

namespace Intropy.Framework.Hosting.Common;

internal static class ActivitySourceProvider
{
    private const string ActivitySourceName = "Intropy.Framework.Hosting";
    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
