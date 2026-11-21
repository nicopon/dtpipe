using System.Buffers.Binary;
using System.Buffers.Text;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

internal static partial class NativeFakeGenerators
{
    private sealed class RandomNumberGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(int);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            switch (targetField.DataType)
            {
                case Int32Type:
                    {
                        var b = new Int32Array.Builder();
                        b.Reserve(rowCount);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            b.Append(rng.NextInt(0, 1_000_001));
                        }
                        return b.Build();
                    }
                case Int64Type:
                    {
                        var b = new Int64Array.Builder();
                        b.Reserve(rowCount);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            b.Append((long)rng.NextInt(0, 1_000_001));
                        }
                        return b.Build();
                    }
                case StringType:
                    {
                        var b = new Utf8ArrowColumnBuilder(rowCount, 7);
                        Span<byte> buf = stackalloc byte[16];
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            Utf8Formatter.TryFormat(rng.NextInt(0, 1_000_001), buf, out int w);
                            b.Append(buf[..w]);
                        }
                        return b.Build();
                    }
                default:
                    {
                        var b = ArrowTypeMapper.CreateBuilder(targetField.DataType);
                        var appender = ArrowTypeMapper.ResolveAppender(b);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            appender(rng.NextInt(0, 1_000_001));
                        }
                        return ArrowTypeMapper.BuildArray(b);
                    }
            }
        }
    }

    private sealed class RandomBoolGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(bool);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is BooleanType)
            {
                var b = new BooleanArray.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append((rng.NextUInt64() & 1) != 0);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 5);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append((rng.NextUInt64() & 1) != 0 ? "True"u8 : "False"u8);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender((rng.NextUInt64() & 1) != 0);
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class UuidGenerator : IFakeColumnGenerator
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;
        public Type NativeClrType => typeof(Guid);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is FixedSizeBinaryType { ByteWidth: 16 })
            {
                var b = new FixedSizeBinaryArrayBuilder(16);
                b.Reserve(rowCount);
                Span<byte> bytes = stackalloc byte[16];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), bytes);
                    b.Append(bytes);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 36);
                Span<byte> raw = stackalloc byte[16];
                Span<byte> text = stackalloc byte[36];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), raw);
                    FormatUuidAscii(raw, text);
                    b.Append(text);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            Span<byte> tmp = stackalloc byte[16];
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), tmp);
                appender(ArrowTypeMap.FromArrowUuidBytes(tmp));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }

        private static void WriteUuidV4Bytes(ulong seed, Span<byte> dest)
        {
            var rng = new SplitMix64(seed);
            BinaryPrimitives.WriteUInt64BigEndian(dest[..8], rng.NextUInt64());
            BinaryPrimitives.WriteUInt64BigEndian(dest[8..], rng.NextUInt64());
            dest[6] = (byte)((dest[6] & 0x0F) | 0x40);
            dest[8] = (byte)((dest[8] & 0x3F) | 0x80);
        }

        private static void FormatUuidAscii(ReadOnlySpan<byte> raw, Span<byte> dest)
        {
            int p = 0;
            for (int i = 0; i < 16; i++)
            {
                if (i is 4 or 6 or 8 or 10)
                    dest[p++] = (byte)'-';
                byte b = raw[i];
                dest[p++] = Hex[b >> 4];
                dest[p++] = Hex[b & 0xF];
            }
        }
    }

    private sealed class GeoCoordGenerator : IFakeColumnGenerator
    {
        private readonly bool _isLatitude;
        public Type NativeClrType => typeof(double);

        public GeoCoordGenerator(bool isLatitude) => _isLatitude = isLatitude;

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            double span = _isLatitude ? 180.0 : 360.0;
            double offset = _isLatitude ? -90.0 : -180.0;

            if (targetField.DataType is DoubleType)
            {
                var b = new DoubleArray.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(Math.Round(rng.NextDouble() * span + offset, 4));
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(Math.Round(rng.NextDouble() * span + offset, 4));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class AmountDecimalGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(decimal);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is Decimal128Type decType)
            {
                var b = new Decimal128Array.Builder(decType);
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    decimal val = rng.NextInt(1, 100_001) / 100m;
                    b.Append(val);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 7);
                Span<byte> buf = stackalloc byte[16];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    int cents = rng.NextInt(1, 100_001);
                    Utf8Formatter.TryFormat(cents / 100, buf, out int p);
                    buf[p++] = (byte)'.';
                    int frac = cents % 100;
                    buf[p++] = (byte)('0' + frac / 10);
                    buf[p++] = (byte)('0' + frac % 10);
                    b.Append(buf[..p]);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(rng.NextInt(1, 100_001) / 100m);
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class DateRangeGenerator : IFakeColumnGenerator
    {
        private readonly long _maxSeconds;
        private readonly bool _past;
        private readonly Func<DateTime> _referenceDateProvider;

        public Type NativeClrType => typeof(DateTime);

        public DateRangeGenerator(TimeSpan span, bool past, Func<DateTime> referenceDateProvider)
        {
            _maxSeconds = Math.Max(1, (long)span.TotalSeconds);
            _past = past;
            _referenceDateProvider = referenceDateProvider;
        }

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            var refDt = _referenceDateProvider();
            long refTicks = refDt.Ticks - (refDt.Ticks % TimeSpan.TicksPerSecond);

            if (targetField.DataType is TimestampType tsType)
            {
                var b = new TimestampArray.Builder(tsType);
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    // Convert via TemporalNormalization so wall-clock DateTime values never shift by local TZ.
                    b.Append(TemporalNormalization.ToOffset(new DateTime(ticks, DateTimeKind.Unspecified)));
                }
                return b.Build();
            }

            if (targetField.DataType is Date32Type)
            {
                var b = new Date32Array.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(new DateTime(ticks, DateTimeKind.Unspecified));
                }
                return b.Build();
            }

            if (targetField.DataType is Date64Type)
            {
                var b = new Date64Array.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(new DateTime(ticks, DateTimeKind.Unspecified));
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(new DateTime(ticks, DateTimeKind.Unspecified));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }

        private long SampleTicks(long refTicks, ulong seed)
        {
            var rng = new SplitMix64(seed);
            long deltaSec = 1 + rng.NextInt64(_maxSeconds);
            long deltaTicks = deltaSec * TimeSpan.TicksPerSecond;
            return _past ? refTicks - deltaTicks : refTicks + deltaTicks;
        }
    }
}
