namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// How a SQL source reads only what changed since the last successful run. Without a delta, a source
/// reads its query's complete result on every run (a full export).
/// </summary>
/// <remarks>
/// The query gets two parameters: <c>@watermark</c>, the committed watermark (exclusive), and
/// <c>@watermarkTo</c>, an upper bound (inclusive): <c>NULL</c> unless the read has one, which it has
/// when it reads again a range a previous run read but did not commit, and on every run of a delta
/// read <see cref="UpToNow">up to now</see>. The watermark moves only after the message has been
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
        return new ColumnDelta(column, settle: null);
    }

    /// <summary>
    /// Reads up to now rather than up to the highest value read: before the query runs, the read's
    /// upper bound is fixed at the current time less <paramref name="settle"/>, and that bound becomes
    /// the new watermark once the read has been handled, also when it found no rows. For a date and
    /// time column stamped when a row is written: a transaction that stamps its rows before a poll and
    /// commits after it is still read, by a later run, as long as it commits within the settle time.
    /// </summary>
    /// <remarks>
    /// The current time is the database's (<c>SYSDATETIMEOFFSET()</c>) on SQL Server, so the
    /// application's clock does not matter; SQLite has none, so there it is the application's
    /// (<see cref="TimeProvider"/>). A <c>datetimeoffset</c> column compares by instant; a
    /// <c>datetime</c>/<c>datetime2</c> column must hold UTC. A run whose bound is not after the
    /// watermark reads nothing and keeps the watermark: it never moves back.
    /// </remarks>
    /// <param name="settle">How long a row's time may lag its commit: longer than the longest
    /// transaction that writes the table. Zero reads up to the clock itself.</param>
    /// <returns>The delta.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="settle"/> is negative.</exception>
    public Delta UpToNow(TimeSpan settle)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(settle, TimeSpan.Zero);
        return new ColumnDelta(WatermarkColumn, settle);
    }

    /// <summary>The result column the watermark is read from.</summary>
    internal abstract string WatermarkColumn { get; }

    /// <summary>How far behind the clock a read up to now ends, or <see langword="null"/> for a delta
    /// that reads up to the highest value read.</summary>
    internal abstract TimeSpan? Settle { get; }

    private sealed class ColumnDelta(string column, TimeSpan? settle) : Delta
    {
        internal override string WatermarkColumn => column;
        internal override TimeSpan? Settle => settle;

        public override string ToString() =>
            settle is { } s ? $"column {column}, up to now less {s}" : $"column {column}";
    }
}
