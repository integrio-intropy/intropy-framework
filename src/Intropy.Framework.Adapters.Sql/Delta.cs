namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// How a SQL source reads only what changed since the last successful run. Without a delta, a source
/// reads its query's complete result on every run (a full export).
/// </summary>
/// <remarks>
/// The query gets two parameters: <c>@watermark</c>, the committed watermark (exclusive), and
/// <c>@watermarkTo</c>, an upper bound (inclusive) that is <c>NULL</c> unless the run reads again a
/// range a previous run read but did not commit. The watermark moves only after the message has been
/// handled.
/// </remarks>
public abstract class Delta
{
    private protected Delta()
    {
    }

    /// <summary>
    /// The watermark is a column of the result: the new watermark is its highest value in the rows
    /// read. The adapter keeps only rows in <c>(@watermark, @watermarkTo]</c> on that column, so the
    /// query need not use <c>@watermarkTo</c>; it should still filter on <c>@watermark</c>
    /// (<c>WHERE MODIFIEDDATETIME &gt; @watermark</c>), or it reads every row on every run. Rows where
    /// the column is <c>NULL</c> are never sent.
    /// </summary>
    /// <param name="column">The column's name in the result, such as <c>MODIFIEDDATETIME</c>. A date
    /// and time, an integer, a decimal, a <c>rowversion</c>/binary or a string column; strings compare
    /// ordinally.</param>
    /// <returns>The delta.</returns>
    public static Delta Column(string column)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(column);
        return new ColumnDelta(column);
    }

    /// <summary>
    /// The query computes the watermark itself and returns it in a column named
    /// <see cref="CursorColumnName"/>: the new watermark is its highest value. That column is not part
    /// of the message. The query must read only rows after <c>@watermark</c>, and, when
    /// <c>@watermarkTo</c> is set, no row beyond it.
    /// </summary>
    /// <returns>The delta.</returns>
    public static Delta Cursor() => new CursorDelta();

    /// <summary>The column a <see cref="Cursor"/> delta's query returns its watermark in.</summary>
    public const string CursorColumnName = "__watermark";

    /// <summary>The result column the watermark is read from.</summary>
    internal abstract string WatermarkColumn { get; }

    /// <summary>Whether the adapter keeps only rows in the range on <see cref="WatermarkColumn"/>, and
    /// whether that column stays in the message.</summary>
    internal abstract bool IsColumn { get; }

    private sealed class ColumnDelta(string column) : Delta
    {
        internal override string WatermarkColumn => column;
        internal override bool IsColumn => true;
        public override string ToString() => $"column {column}";
    }

    private sealed class CursorDelta : Delta
    {
        internal override string WatermarkColumn => CursorColumnName;
        internal override bool IsColumn => false;
        public override string ToString() => $"cursor {CursorColumnName}";
    }
}
