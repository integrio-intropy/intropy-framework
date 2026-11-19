namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// Context metadata a SQL source sets on its message, next to the source port and the pickup time
/// every source sets. There is no file name.
/// </summary>
public static class SqlContextKeys
{
    /// <summary>The number of rows in the message.</summary>
    public const string RowCount = "sql.rowCount";

    /// <summary>With a delta: the watermark the read started after (exclusive).</summary>
    public const string WatermarkFrom = "sql.watermark.from";

    /// <summary>With a delta: the watermark the read went up to (inclusive); equal to
    /// <see cref="WatermarkFrom"/> when nothing new was read.</summary>
    public const string WatermarkTo = "sql.watermark.to";
}
