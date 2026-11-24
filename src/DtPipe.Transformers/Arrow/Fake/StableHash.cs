using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Mapping;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Fast, statelessly seedable 64-bit PRNG based on SplitMix64.
/// </summary>
internal struct SplitMix64
{
    private ulong _state;

    public SplitMix64(ulong seed)
    {
        _state = seed;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Mix64(ulong z)
    {
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong NextUInt64()
    {
        _state += 0x9E3779B97F4A7C15UL;
        return Mix64(_state);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int NextInt(int maxExclusive)
    {
        if (maxExclusive <= 1) return 0;
        return (int)(((ulong)(uint)NextUInt64() * (ulong)(uint)maxExclusive) >> 32);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int NextInt(int minInclusive, int maxExclusive)
    {
        long range = (long)maxExclusive - minInclusive;
        if (range <= 1) return minInclusive;
        if (range <= uint.MaxValue)
            return minInclusive + (int)(((ulong)(uint)NextUInt64() * (ulong)range) >> 32);
        return minInclusive + (int)(NextUInt64() % (ulong)range);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long NextInt64(long maxExclusive)
    {
        if (maxExclusive <= 1) return 0;
        return (long)(NextUInt64() % (ulong)maxExclusive);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long NextInt64(long minInclusive, long maxExclusive)
    {
        ulong range = (ulong)(maxExclusive - minInclusive);
        if (range <= 1) return minInclusive;
        return minInclusive + (long)(NextUInt64() % range);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double NextDouble() => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

    public void NextBytes(Span<byte> destination)
    {
        int i = 0;
        while (i + 8 <= destination.Length)
        {
            ulong v = NextUInt64();
            MemoryMarshal.Write(destination.Slice(i, 8), in v);
            i += 8;
        }
        if (i < destination.Length)
        {
            ulong v = NextUInt64();
            while (i < destination.Length)
            {
                destination[i++] = (byte)v;
                v >>= 8;
            }
        }
    }
}

/// <summary>
/// Provides stable 32-bit and 64-bit hash computation for deterministic faker seeding.
/// </summary>
public static class StableHash
{
    private const uint FnvPrime = 16777619;
    private const uint FnvOffsetBasis = 2166136261;

    private const ulong Fnv64Prime = 1099511628211UL;
    private const ulong Fnv64OffsetBasis = 14695981039346656037UL;

    public static uint Compute(object? value)
    {
        if (value is null) return 0;

        if (value is int i) return Compute(i);
        if (value is long l) return Compute(l);
        if (value is string s) return ComputeFromString(s);

        return ComputeFromString(value.ToString()!);
    }

    public static uint Compute(int value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            hash ^= (uint)(value & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 8) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 16) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 24) & 0xFF); hash *= FnvPrime;
            return hash;
        }
    }

    public static uint Compute(long value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            hash ^= (uint)(value & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 8) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 16) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 24) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 32) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 40) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 48) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 56) & 0xFF); hash *= FnvPrime;
            return hash;
        }
    }

    public static uint ComputeFromString(string value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                hash ^= (uint)(c & 0xFF);
                hash *= FnvPrime;
                hash ^= (uint)((c >> 8) & 0xFF);
                hash *= FnvPrime;
            }
            return hash;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Compute64(ReadOnlySpan<byte> bytes)
    {
        ulong hash = Fnv64OffsetBasis;
        for (int i = 0; i < bytes.Length; i++)
        {
            hash ^= bytes[i];
            hash *= Fnv64Prime;
        }
        return SplitMix64.Mix64(hash);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Compute64(long value) => SplitMix64.Mix64(unchecked((ulong)value));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Compute64(ulong value) => SplitMix64.Mix64(value);

    internal static ulong Compute64CaseInsensitive(string value)
    {
        ulong hash = Fnv64OffsetBasis;
        for (int i = 0; i < value.Length; i++)
        {
            char c = char.ToLowerInvariant(value[i]);
            hash ^= (byte)(c & 0xFF);
            hash *= Fnv64Prime;
            hash ^= (byte)(c >> 8);
            hash *= Fnv64Prime;
        }
        return SplitMix64.Mix64(hash);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Combine64(ulong accumulator, int laneIndex, ulong value)
        => SplitMix64.Mix64(accumulator ^ value ^ unchecked((ulong)(laneIndex + 1) * 0x9E3779B97F4A7C15UL));
}

/// <summary>
/// Extracts 64-bit deterministic row seed keys directly from Arrow column buffers without boxing
/// or a managed string allocation per row.
/// <para>
/// A key is a function of the value <see cref="ArrowTypeMap.CompileValueReader"/> reads from the
/// column, never of how the column stores it: one value is one key whatever its integer width, its
/// timestamp unit or zone, its date width or its dictionary encoding. A text hashes as its UTF-8
/// bytes, a date, a wall clock and an instant as the ticks of the moment they name, so that
/// <c>--fake-seed-column</c> keeps a join between two tables whose columns store the value apart.
/// </para>
/// </summary>
internal static class ArrowSeedKeyExtractor
{
    internal const ulong InitialAccumulator = 0x9E3779B97F4A7C15UL;
    internal const ulong NullLaneSentinel = 0xD6E8FEB86659FD93UL;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ComputeDecimal64(decimal value)
    {
        var bare = DecimalWidth.Bare(value);
        if (bare == 0m) bare = 0m;
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(bare, bits);
        return StableHash.Compute64(MemoryMarshal.AsBytes(bits));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong DoubleKey(double value) => StableHash.Compute64(value == 0.0 ? 0L : BitConverter.DoubleToInt64Bits(value));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong FloatKey(float value) => StableHash.Compute64(value == 0.0f ? 0L : BitConverter.SingleToInt32Bits(value));

    /// <summary>A date is the ticks of its midnight, as a <see cref="DateTime"/> of that day would be.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong DateKey(DateOnly value) => StableHash.Compute64(value.DayNumber * TimeSpan.TicksPerDay);

    /// <summary>
    /// A <see cref="DateTime"/> is its own ticks: a wall clock has no zone to apply, and an instant
    /// marked local is read as the UTC moment it names, so that the host's zone is never in a key.
    /// </summary>
    private static ulong DateTimeKey(DateTime value)
        => StableHash.Compute64(value.Kind == DateTimeKind.Local ? value.ToUniversalTime().Ticks : value.Ticks);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong InstantKey(DateTimeOffset value) => StableHash.Compute64(value.UtcTicks);

    /// <summary>
    /// The key of a value read as a boxed object, for a cell that is not read in place: it gives the
    /// key the typed loop of the same value gives.
    /// </summary>
    private static ulong HashValue(object value) => value switch
    {
        string s => StableHash.Compute64(Encoding.UTF8.GetBytes(s)),
        bool b => StableHash.Compute64(b ? 1UL : 0UL),
        sbyte v => StableHash.Compute64((long)v),
        short v => StableHash.Compute64((long)v),
        int v => StableHash.Compute64((long)v),
        long v => StableHash.Compute64(v),
        byte v => StableHash.Compute64((long)v),
        ushort v => StableHash.Compute64((long)v),
        uint v => StableHash.Compute64((long)v),
        ulong v => StableHash.Compute64(v),
        double v => DoubleKey(v),
        float v => FloatKey(v),
        decimal v => ComputeDecimal64(v),
        DateOnly v => DateKey(v),
        DateTime v => DateTimeKey(v),
        DateTimeOffset v => InstantKey(v),
        byte[] v => StableHash.Compute64(v),
        _ => DisplayKey(value)
    };

    /// <summary>The key of a value with no form of its own: the UTF-16 bytes of its text.</summary>
    private static ulong DisplayKey(object value)
    {
        var text = value as string ?? value.ToString() ?? string.Empty;
        return StableHash.Compute64(MemoryMarshal.AsBytes(text.AsSpan()));
    }

    public static void FillRowKeys(RecordBatch batch, int[] seedColumnIndices, ulong[] keys, bool[] hasKey)
    {
        int rowCount = batch.Length;
        keys.AsSpan(0, rowCount).Fill(InitialAccumulator);
        hasKey.AsSpan(0, rowCount).Clear();

        for (int lane = 0; lane < seedColumnIndices.Length; lane++)
        {
            int colIdx = seedColumnIndices[lane];
            var array = batch.Column(colIdx);
            var field = batch.Schema.GetFieldByIndex(colIdx);
            AccumulateColumn(array, field, lane, rowCount, keys, hasKey);
        }
    }

    private static void AccumulateColumn(IArrowArray array, Field field, int lane, int rowCount, ulong[] keys, bool[] hasKey)
    {
        bool noNulls = array.NullCount == 0;

        switch (array)
        {
            case StringArray a:
                for (int i = 0; i < rowCount; i++)
                {
                    var bytes = a.GetBytes(i, out bool isNull);
                    if (isNull)
                    {
                        keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    }
                    else
                    {
                        keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(bytes));
                        hasKey[i] = true;
                    }
                }
                break;

            case Int64Array a:
                {
                    var values = a.Values;
                    if (noNulls)
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(values[i]));
                            hasKey[i] = true;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                            else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(values[i])); hasKey[i] = true; }
                        }
                    }
                    break;
                }

            case Int32Array a:
                {
                    var values = a.Values;
                    if (noNulls)
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i]));
                            hasKey[i] = true;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                            else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                        }
                    }
                    break;
                }

            case Int16Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case Int8Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case UInt64Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case UInt32Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case UInt16Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case UInt8Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case DoubleArray a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, DoubleKey(values[i]));
                            hasKey[i] = true;
                        }
                    }
                    break;
                }

            case FloatArray a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, FloatKey(values[i]));
                            hasKey[i] = true;
                        }
                    }
                    break;
                }

            case BooleanArray a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else
                    {
                        ulong v = a.GetValue(i) == true ? 1UL : 0UL;
                        keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(v));
                        hasKey[i] = true;
                    }
                }
                break;

            case BinaryArray a:
                for (int i = 0; i < rowCount; i++)
                {
                    var bytes = a.GetBytes(i, out bool isNull);
                    if (isNull) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(bytes)); hasKey[i] = true; }
                }
                break;

            // Decimal128Array/Decimal256Array derive from FixedSizeBinaryArray and must precede it.
            case Decimal128Array a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i))
                    {
                        keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    }
                    else
                    {
                        ulong valHash;
                        try
                        {
                            valHash = ComputeDecimal64(a.GetValue(i)!.Value);
                        }
                        catch (OverflowException)
                        {
                            valHash = StableHash.Compute64(a.GetBytes(i));
                        }
                        keys[i] = StableHash.Combine64(keys[i], lane, valHash);
                        hasKey[i] = true;
                    }
                }
                break;

            case Decimal256Array a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i))
                    {
                        keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    }
                    else
                    {
                        ulong valHash;
                        try
                        {
                            valHash = ComputeDecimal64(a.GetValue(i)!.Value);
                        }
                        catch (OverflowException)
                        {
                            valHash = StableHash.Compute64(a.GetBytes(i));
                        }
                        keys[i] = StableHash.Combine64(keys[i], lane, valHash);
                        hasKey[i] = true;
                    }
                }
                break;

            case FixedSizeBinaryArray a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(a.GetBytes(i))); hasKey[i] = true; }
                }
                break;

            case Date32Array a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else { keys[i] = StableHash.Combine64(keys[i], lane, DateKey(a.GetDateOnly(i)!.Value)); hasKey[i] = true; }
                }
                break;

            case Date64Array a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else { keys[i] = StableHash.Combine64(keys[i], lane, DateTimeKey(a.GetDateTime(i)!.Value)); hasKey[i] = true; }
                }
                break;

            // The unit and the zone are storage: the key is the instant the reader returns, which a
            // zone-less column reads as the wall clock of the same ticks.
            case TimestampArray a:
                for (int i = 0; i < rowCount; i++)
                {
                    if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                    else { keys[i] = StableHash.Combine64(keys[i], lane, InstantKey(a.GetTimestamp(i)!.Value)); hasKey[i] = true; }
                }
                break;

            case DictionaryArray a:
                AccumulateDictionary(a, lane, rowCount, keys, hasKey);
                break;

            default:
                {
                    var reader = ArrowTypeMap.CompileValueReader(array, field);
                    for (int i = 0; i < rowCount; i++)
                    {
                        var val = reader(i);
                        if (val is null)
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        }
                        else
                        {
                            keys[i] = StableHash.Combine64(keys[i], lane, DisplayKey(val));
                            hasKey[i] = true;
                        }
                    }
                    break;
                }
        }
    }

    /// <summary>
    /// A dictionary column is the values its indices point at: each entry is hashed once, as the value
    /// <see cref="ArrowTypeMap.GetValue"/> reads from the dictionary, and a row takes the key of its
    /// entry. A null index, a null entry and an index out of range are a null, as the reader reads them.
    /// </summary>
    private static void AccumulateDictionary(DictionaryArray array, int lane, int rowCount, ulong[] keys, bool[] hasKey)
    {
        var dictionary = array.Dictionary;
        var entryKeys = new ulong[dictionary.Length];
        var entryPresent = new bool[dictionary.Length];
        for (int j = 0; j < entryKeys.Length; j++)
        {
            if (ArrowTypeMap.GetValue(dictionary, j) is { } entry)
            {
                entryKeys[j] = HashValue(entry);
                entryPresent[j] = true;
            }
        }

        var indices = array.Indices;
        for (int i = 0; i < rowCount; i++)
        {
            int position = indices.IsNull(i) ? -1 : PositionAt(indices, i);
            if ((uint)position < (uint)entryPresent.Length && entryPresent[position])
            {
                keys[i] = StableHash.Combine64(keys[i], lane, entryKeys[position]);
                hasKey[i] = true;
            }
            else
            {
                keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
            }
        }
    }

    private static int PositionAt(IArrowArray indices, int i) => indices switch
    {
        Int8Array a => a.Values[i],
        Int16Array a => a.Values[i],
        Int32Array a => a.Values[i],
        Int64Array a => checked((int)a.Values[i]),
        UInt8Array a => a.Values[i],
        UInt16Array a => a.Values[i],
        UInt32Array a => checked((int)a.Values[i]),
        UInt64Array a => checked((int)a.Values[i]),
        _ => Convert.ToInt32(ArrowTypeMap.GetValue(indices, i), CultureInfo.InvariantCulture)
    };
}
