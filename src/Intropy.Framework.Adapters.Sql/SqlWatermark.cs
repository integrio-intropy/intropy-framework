using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.Data.SqlClient;

namespace Intropy.Framework.Adapters.Sql;

/// <summary>
/// A watermark value with its type, so it is passed to the query as a parameter of the right type
/// and compared with the column's values the way SQL Server compares them. Stored as text tagged
/// with its type (<c>datetime:2026-11-20T01:50:00.0000000</c>), which the cursor store keeps opaque.
/// </summary>
internal sealed class SqlWatermark : IComparable<SqlWatermark>
{
    private const string DateTimeTag = "datetime";
    private const string DateTimeOffsetTag = "datetimeoffset";
    private const string IntegerTag = "int";
    private const string DecimalTag = "decimal";
    private const string BinaryTag = "binary";
    private const string StringTag = "string";

    private SqlWatermark(string tag, object value, string? written = null)
    {
        Tag = tag;
        Value = value;
        Written = written;
    }

    private string Tag { get; }

    /// <summary>The text an untagged initial watermark was written as, so that reading it as another
    /// type (a string column's <c>001</c>, inferred as the integer 1) starts from what was written, not
    /// from the inferred value.</summary>
    private string? Written { get; }

    /// <summary>The value: a <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="long"/>,
    /// <see cref="decimal"/>, <see cref="byte"/> array or <see cref="string"/>.</summary>
    internal object Value { get; }

    /// <summary>The watermark of a column value read from the result.</summary>
    /// <exception cref="InvalidOperationException">The value's type cannot be a watermark.</exception>
    internal static SqlWatermark FromValue(object value, string column) => value switch
    {
        DateTime v => new SqlWatermark(DateTimeTag, v),
        DateTimeOffset v => new SqlWatermark(DateTimeOffsetTag, v),
        long v => new SqlWatermark(IntegerTag, v),
        int v => new SqlWatermark(IntegerTag, (long)v),
        short v => new SqlWatermark(IntegerTag, (long)v),
        byte v => new SqlWatermark(IntegerTag, (long)v),
        decimal v => new SqlWatermark(DecimalTag, v),
        // SQLite returns a REAL column as a double.
        double v => new SqlWatermark(DecimalTag, (decimal)v),
        byte[] v => new SqlWatermark(BinaryTag, v),
        string v => new SqlWatermark(StringTag, v),
        _ => throw new InvalidOperationException(
            $"The watermark column '{column}' is of type {value.GetType().Name}; a watermark is a date and time, " +
            "an integer, a decimal, a rowversion/binary or a string.")
    };

    /// <summary>Reads a stored watermark or a configured initial watermark. Either is tagged with its
    /// type (<c>datetime:</c>, <c>datetimeoffset:</c>, <c>int:</c>, <c>decimal:</c>, <c>binary:</c>,
    /// <c>string:</c>), which decides the parameter's SQL type; an untagged initial watermark is
    /// inferred: an integer written without leading zeros, a <c>0x</c> hex binary, a date and time
    /// (ISO 8601), or else a string.</summary>
    /// <exception cref="FormatException">A tagged value is not of its tag's type.</exception>
    /// <exception cref="OverflowException">A tagged number is out of range.</exception>
    internal static SqlWatermark Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var separator = text.IndexOf(':', StringComparison.Ordinal);
        if (separator > 0 && TryParseTagged(text[..separator], text[(separator + 1)..]) is { } tagged)
            return tagged;
        return Infer(text);
    }

    private static SqlWatermark? TryParseTagged(string tag, string value) => tag switch
    {
        DateTimeTag => new SqlWatermark(tag, DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)),
        DateTimeOffsetTag => new SqlWatermark(tag, DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)),
        IntegerTag => new SqlWatermark(tag, long.Parse(value, CultureInfo.InvariantCulture)),
        DecimalTag => new SqlWatermark(tag, decimal.Parse(value, CultureInfo.InvariantCulture)),
        BinaryTag => new SqlWatermark(tag, Convert.FromHexString(
            value.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? value.AsSpan(2) : value.AsSpan())),
        StringTag => new SqlWatermark(tag, value),
        _ => null
    };

    private static SqlWatermark Infer(string text)
    {
        var trimmed = text.Trim();
        // Only an integer as an integer column writes it: "001" or "+5" is more likely a string's.
        if (long.TryParse(trimmed, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var integer)
            && string.Equals(integer.ToString(CultureInfo.InvariantCulture), trimmed, StringComparison.Ordinal))
            return new SqlWatermark(IntegerTag, integer, text);
        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase) && trimmed.Length > 2 && trimmed.Length % 2 == 0)
        {
            try
            {
                return new SqlWatermark(BinaryTag, Convert.FromHexString(trimmed.AsSpan(2)), text);
            }
            catch (FormatException)
            {
                // Not hex after all: a string.
            }
        }

        if (trimmed.Contains('-', StringComparison.Ordinal)
            && DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
        {
            // With an explicit offset it is a datetimeoffset; without one, a local date and time as a
            // datetime/datetime2 column holds it.
            return HasOffset(trimmed)
                ? new SqlWatermark(DateTimeOffsetTag, DateTimeOffset.Parse(trimmed, CultureInfo.InvariantCulture), text)
                : new SqlWatermark(DateTimeTag, DateTime.Parse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None), text);
        }

        return new SqlWatermark(StringTag, text);
    }

    private static bool HasOffset(string text)
    {
        var time = text.IndexOf('T', StringComparison.Ordinal);
        if (time < 0)
            time = text.IndexOf(' ', StringComparison.Ordinal);
        if (time < 0)
            return false;
        var clock = text[(time + 1)..];
        return clock.EndsWith('Z') || clock.Contains('+', StringComparison.Ordinal) || clock.Contains('-', StringComparison.Ordinal);
    }

    /// <summary>This watermark as the type of <paramref name="other"/>, so a configured initial watermark
    /// (inferred from text) compares with the column's values.</summary>
    /// <exception cref="InvalidOperationException">This watermark cannot be read as that type.</exception>
    internal SqlWatermark As(SqlWatermark other, string column)
    {
        if (Tag == other.Tag)
            return this;
        try
        {
            return (Tag, other.Tag) switch
            {
                (DateTimeTag, DateTimeOffsetTag) => new SqlWatermark(other.Tag, new DateTimeOffset((DateTime)Value, TimeSpan.Zero)),
                (DateTimeOffsetTag, DateTimeTag) => new SqlWatermark(other.Tag, ((DateTimeOffset)Value).UtcDateTime),
                (IntegerTag, DecimalTag) => new SqlWatermark(other.Tag, (decimal)(long)Value),
                (DecimalTag, IntegerTag) when decimal.Truncate((decimal)Value) == (decimal)Value =>
                    new SqlWatermark(other.Tag, (long)(decimal)Value),
                (IntegerTag, BinaryTag) => new SqlWatermark(other.Tag, BigEndian((long)Value)),
                _ => TryParseTagged(other.Tag, Written ?? Text) ?? throw new FormatException()
            };
        }
        catch (Exception e) when (e is FormatException or OverflowException or InvalidCastException)
        {
            throw new InvalidOperationException(
                $"The watermark cannot be compared with the values of the column '{column}' ({other.Tag}); " +
                $"it is a {Tag}. Check InitialWatermark, or tag it with the column's type ({other.Tag}:...).", e);
        }
    }

    private static byte[] BigEndian(long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return bytes;
    }

    /// <summary>The value as text, without its type: for logs and the context metadata.</summary>
    internal string Text => Value switch
    {
        DateTime v => v.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset v => v.ToString("O", CultureInfo.InvariantCulture),
        long v => v.ToString(CultureInfo.InvariantCulture),
        decimal v => v.ToString(CultureInfo.InvariantCulture),
        byte[] v => "0x" + Convert.ToHexString(v),
        _ => (string)Value
    };

    /// <summary>The value as stored: text tagged with its type.</summary>
    internal string Stored => $"{Tag}:{Text}";

    /// <summary>
    /// <paramref name="value"/> as the parameter <paramref name="name"/> of <paramref name="command"/>,
    /// or a <c>NULL</c> of <paramref name="type"/>'s type when there is no value. SQL Server gets the
    /// watermark's SQL type. SQLite has no declared types: a date and time is passed as ISO 8601 text,
    /// so it compares with a TEXT column holding ISO 8601 values.
    /// </summary>
    internal static DbParameter Parameter(DbCommand command, string name, SqlWatermark? value, SqlWatermark? type)
    {
        if (command is SqlCommand)
        {
            return new SqlParameter(name, (value ?? type)?.DbType ?? SqlDbType.NVarChar)
            {
                Value = value?.Value ?? DBNull.Value
            };
        }

        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value?.Value switch
        {
            null => DBNull.Value,
            DateTime or DateTimeOffset => value.Text,
            var v => v
        };
        return parameter;
    }

    private SqlDbType DbType => Tag switch
    {
        DateTimeTag => SqlDbType.DateTime2,
        DateTimeOffsetTag => SqlDbType.DateTimeOffset,
        IntegerTag => SqlDbType.BigInt,
        DecimalTag => SqlDbType.Decimal,
        BinaryTag => SqlDbType.VarBinary,
        _ => SqlDbType.NVarChar
    };

    /// <inheritdoc />
    /// <remarks>Both watermarks must have the same type (<see cref="As"/>).</remarks>
    public int CompareTo(SqlWatermark? other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Value switch
        {
            DateTime v => v.CompareTo((DateTime)other.Value),
            DateTimeOffset v => v.CompareTo((DateTimeOffset)other.Value),
            long v => v.CompareTo((long)other.Value),
            decimal v => v.CompareTo((decimal)other.Value),
            byte[] v => CompareBinary(v, (byte[])other.Value),
            _ => string.CompareOrdinal((string)Value, (string)other.Value)
        };
    }

    // SQL Server compares binary values byte by byte, a shorter value padded with zeros on the right.
    private static int CompareBinary(byte[] left, byte[] right)
    {
        for (var i = 0; i < Math.Max(left.Length, right.Length); i++)
        {
            var a = i < left.Length ? left[i] : (byte)0;
            var b = i < right.Length ? right[i] : (byte)0;
            if (a != b)
                return a.CompareTo(b);
        }

        return 0;
    }

    /// <inheritdoc />
    public override string ToString() => Text;
}
