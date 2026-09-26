using System.Diagnostics;
using System.Reflection;

namespace Intropy.Framework.Core.Common;

internal static class ActivitySourceProvider
{
    public const string ActivitySourceName = "Intropy.Framework.Core";
    private static readonly string? Version = typeof(ActivitySourceProvider).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, Version);
}
