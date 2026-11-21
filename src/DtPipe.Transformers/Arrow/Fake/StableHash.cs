using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
/// or managed string allocations.
/// </summary>
internal static class ArrowSeedKeyExtractor
{
    private const ulong InitialAccumulator = 0x9E3779B97F4A7C15UL;
    private const ulong NullLaneSentinel = 0xD6E8FEB86659FD93UL;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ComputeDecimal64(decimal value)
    {
        var bare = DecimalWidth.Bare(value);
        if (bare == 0m) bare = 0m;
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(bare, bits);
        return StableHash.Compute64(MemoryMarshal.AsBytes(bits));
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
                            double d = values[i];
                            long bits = d == 0.0 ? 0L : BitConverter.DoubleToInt64Bits(d);
                            keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(bits));
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
                            float f = values[i];
                            long bits = f == 0.0f ? 0L : BitConverter.SingleToInt32Bits(f);
                            keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(bits));
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
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64((long)values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case Date64Array a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(values[i])); hasKey[i] = true; }
                    }
                    break;
                }

            case TimestampArray a:
                {
                    var values = a.Values;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (!noNulls && a.IsNull(i)) keys[i] = StableHash.Combine64(keys[i], lane, NullLaneSentinel);
                        else { keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(values[i])); hasKey[i] = true; }
                    }
                    break;
                }

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
                            var str = val as string ?? val.ToString() ?? string.Empty;
                            keys[i] = StableHash.Combine64(keys[i], lane, StableHash.Compute64(MemoryMarshal.AsBytes(str.AsSpan())));
                            hasKey[i] = true;
                        }
                    }
                    break;
                }
        }
    }
}
