using Intropy.Framework.Hosting.StatefulSweeps;
using Microsoft.Extensions.Configuration;

namespace Intropy.Framework.Hosting.Test.StatefulSweeps;

/// <summary>
/// A polled port's cursor store is named by the system's host, in the port's section
/// (<c>Ports:&lt;port&gt;:CursorStore</c>): <see cref="CursorOptions.FromPort"/> reads it and fails
/// naming the key when it is missing.
/// </summary>
public class CursorOptionsTests
{
    private static IConfiguration Configuration(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();

    [Fact]
    public void FromPort_ReadsTheStoreFromThePortSection()
    {
        var configuration = Configuration(("Ports:campaigns:CursorStore", "campaign-extractor-cursors"));

        var options = CursorOptions.FromPort(configuration, "campaigns", initial: "0", canPin: false);

        Assert.Equal(new CursorOptions("campaign-extractor-cursors", "0", CanPin: false), options);
    }

    [Fact]
    public void FromPort_Defaults_ReadFromTheBeginning_AndPin()
    {
        var configuration = Configuration(("Ports:campaigns:CursorStore", "campaign-extractor-cursors"));

        var options = CursorOptions.FromPort(configuration, "campaigns");

        Assert.Equal(new CursorOptions("campaign-extractor-cursors", null), options);
        Assert.True(options.CanPin);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void FromPort_WithoutAStore_FailsNamingTheKey(string? store)
    {
        var configuration = store is null
            ? Configuration(("Ports:other:CursorStore", "other-cursors"))
            : Configuration(("Ports:campaigns:CursorStore", store));

        var exception = Assert.Throws<InvalidOperationException>(() => CursorOptions.FromPort(configuration, "campaigns"));

        Assert.Contains("Ports:campaigns:CursorStore", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreKey_IsTheKeyTheTopologySets()
    {
        Assert.Equal("CursorStore", CursorOptions.StoreKey);
    }
}
