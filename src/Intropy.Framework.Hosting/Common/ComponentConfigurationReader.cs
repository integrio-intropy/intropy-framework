using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace Intropy.Framework.Hosting.Common;

/// <summary>Reads individual option values out of an <see cref="IConfiguration"/> section for the
/// configuration-based registration overloads. Deliberately key-by-key: no binder dependency, and
/// every value is validated here, at registration, with the member it configures named — so
/// configuration cannot smuggle a malformed value past the checks the lambda overloads apply.</summary>
internal static class ComponentConfigurationReader
{
    /// <summary>Reads a string member, e.g. <c>PubSubName</c>. Returns null when the section does
    /// not carry the key, so code-registered values survive untouched.</summary>
    public static string? String(IConfiguration configuration, string key) => configuration[key];

    /// <summary>Reads a <see cref="TimeSpan"/> member, e.g. <c>MaxMessageProcessingTime</c>.
    /// Returns null when the section does not carry the key.</summary>
    /// <exception cref="InvalidOperationException">The key is present but not a valid time span
    /// (<c>d.hh:mm:ss</c>; <c>00:01:00</c> is one minute).</exception>
    public static TimeSpan? TimeSpan(IConfiguration configuration, string key, string targetMember)
    {
        var raw = configuration[key];
        if (string.IsNullOrWhiteSpace(raw))
            return null;
        if (System.TimeSpan.TryParse(raw, CultureInfo.InvariantCulture, out var value))
            return value;
        throw new InvalidOperationException(
            $"{targetMember}: configuration value '{raw}' for '{key}' is not a valid time span " +
            "(set it as 'd.hh:mm:ss', e.g. '00:01:00' for one minute).");
    }

    /// <summary>Reads an <see cref="int"/> member, e.g. <c>CallbackPort</c>. Returns null when the
    /// section does not carry the key.</summary>
    /// <exception cref="InvalidOperationException">The key is present but not an integer.</exception>
    public static int? Int32(IConfiguration configuration, string key, string targetMember)
    {
        var raw = configuration[key];
        if (string.IsNullOrWhiteSpace(raw))
            return null;
        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            return value;
        throw new InvalidOperationException(
            $"{targetMember}: configuration value '{raw}' for '{key}' is not an integer.");
    }

    /// <summary>Reads an enum member by name, case-insensitively, e.g. <c>Unrouted</c>. Returns
    /// null when the section does not carry the key.</summary>
    /// <exception cref="InvalidOperationException">The key is present but names no
    /// <typeparamref name="T"/> value.</exception>
    public static T? Enum<T>(IConfiguration configuration, string key, string targetMember) where T : struct, Enum
    {
        var raw = configuration[key];
        if (string.IsNullOrWhiteSpace(raw))
            return null;
        if (System.Enum.TryParse<T>(raw, ignoreCase: true, out var value))
            return value;
        throw new InvalidOperationException(
            $"{targetMember}: configuration value '{raw}' for '{key}' is not a {typeof(T).Name} value " +
            $"({string.Join(", ", System.Enum.GetNames<T>())}).");
    }
}
