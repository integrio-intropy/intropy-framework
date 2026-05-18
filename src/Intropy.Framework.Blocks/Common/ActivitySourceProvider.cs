using System.Diagnostics;

namespace Intropy.Framework.Blocks.Common;

internal static class ActivitySourceProvider
{
    private const string ActivitySourceName = "Intropy.Framework.Blocks";
    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
