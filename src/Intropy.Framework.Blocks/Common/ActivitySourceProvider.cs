using System.Diagnostics;
using System.Reflection;

namespace Intropy.Framework.Blocks.Common;

internal static class ActivitySourceProvider
{
    private const string ActivitySourceName = "Intropy.Framework.Blocks";
    private static readonly string? Version = typeof(ActivitySourceProvider).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName, Version);
}
