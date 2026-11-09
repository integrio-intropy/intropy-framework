namespace Intropy.Framework.Adapters.Sql.Test;

/// <summary>
/// A watermark keeps its type through the store, a configured initial watermark is read as the type
/// its text looks like, and compares with a column's values once read as the column's type.
/// </summary>
public class SqlWatermarkTests
{
    [Theory]
    [InlineData("2026-11-20T01:50:00", typeof(DateTime))]
    [InlineData("2026-11-20T01:50:00+01:00", typeof(DateTimeOffset))]
    [InlineData("2026-11-20T01:50:00Z", typeof(DateTimeOffset))]
    [InlineData("12345", typeof(long))]
    [InlineData("0x00000000000007D1", typeof(byte[]))]
    [InlineData("ABC-123", typeof(string))]
    public void Parse_InfersTheTypeOfAnUntaggedWatermark(string text, Type type) =>
        Assert.IsType(type, SqlWatermark.Parse(text).Value);

    [Fact]
    public void Stored_RoundTripsWithItsType()
    {
        var values = new object[]
        {
            new DateTime(2026, 11, 20, 1, 50, 0, 123, DateTimeKind.Unspecified).AddTicks(4567),
            new DateTimeOffset(2026, 11, 20, 1, 50, 0, TimeSpan.FromHours(1)),
            42L, 12.5m, new byte[] { 0, 0, 0, 0, 0, 0, 7, 0xD1 }, "1234"
        };

        foreach (var value in values)
        {
            var watermark = SqlWatermark.FromValue(value, "c");
            var read = SqlWatermark.Parse(watermark.Stored);
            Assert.Equal(value, read.Value);
        }
    }

    [Fact]
    public void AStringThatLooksLikeANumber_StaysAString_OnceStored() =>
        Assert.IsType<string>(SqlWatermark.Parse(SqlWatermark.FromValue("1234", "c").Stored).Value);

    [Fact]
    public void AnInitialWatermark_ComparesWithADateTimeOffsetColumn()
    {
        var initial = SqlWatermark.Parse("2026-11-20T01:50:00");
        var later = SqlWatermark.FromValue(new DateTimeOffset(2026, 11, 20, 2, 0, 0, TimeSpan.Zero), "c");

        Assert.True(later.CompareTo(initial.As(later, "c")) > 0);
    }

    [Fact]
    public void AnInitialInteger_ComparesWithARowversion()
    {
        var initial = SqlWatermark.Parse("2000");
        var rowversion = SqlWatermark.FromValue(new byte[] { 0, 0, 0, 0, 0, 0, 7, 0xD1 }, "c"); // 2001

        Assert.True(rowversion.CompareTo(initial.As(rowversion, "c")) > 0);
    }

    [Fact]
    public void AWatermarkThatCannotBeReadAsTheColumnsType_FailsNamingTheColumn()
    {
        var initial = SqlWatermark.Parse("not-a-date");
        var column = SqlWatermark.FromValue(DateTime.UtcNow, "MODIFIEDDATETIME");

        var error = Assert.Throws<InvalidOperationException>(() => initial.As(column, "MODIFIEDDATETIME"));
        Assert.Contains("'MODIFIEDDATETIME'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AColumnOfAnotherType_CannotBeAWatermark() =>
        Assert.Throws<InvalidOperationException>(() => SqlWatermark.FromValue(Guid.NewGuid(), "c"));
}
