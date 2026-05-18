using System.Diagnostics;

namespace Intropy.Framework.Core.Common;

internal static class ActivitySourceProvider
{
    public const string ActivitySourceName = "Intropy.Framework.Core";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
