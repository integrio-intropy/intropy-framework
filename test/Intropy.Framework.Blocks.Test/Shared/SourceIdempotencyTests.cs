using Intropy.Framework.Blocks.Shared;

namespace Intropy.Framework.Blocks.Test.Shared;

/// <summary>
/// Idempotency keyed on a polled source's read comes from the context metadata the sweep sets: the
/// range as the id and the pickup time as the date. A context without the key (a full export) fails
/// with an explanation instead of checking an empty id.
/// </summary>
public class SourceIdempotencyTests
{
    [Fact]
    public void TheIdAndDate_ComeFromTheMetadata()
    {
        var context = new Context(new Dictionary<string, string>
        {
            [SourceContextKeys.IdempotencyKey] = "campaigns:w0:w1",
            [SourceContextKeys.ReceivedAt] = "2026-11-20T01:50:00.0000000+00:00"
        });

        Assert.Equal("campaigns:w0:w1", SourceIdempotency.Id(context));
        Assert.Equal(new DateTimeOffset(2026, 11, 20, 1, 50, 0, TimeSpan.Zero), SourceIdempotency.Date(context));
    }

    [Fact]
    public void WithoutTheKey_TheIdFails_NamingWhy()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            SourceIdempotency.Id(new Context(new Dictionary<string, string>())));

        Assert.Contains("full export", error.Message, StringComparison.Ordinal);
    }
}
