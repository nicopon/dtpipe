using Apache.Arrow;
using Apache.Arrow.Types;
using Bogus;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

internal sealed class FastSeedableRandom : System.Random
{
    private SplitMix64 _rng;

    public FastSeedableRandom() : base(0)
    {
        _rng = new SplitMix64(1UL);
    }

    public void Reseed(ulong seed) => _rng = new SplitMix64(seed);

    protected override double Sample() => _rng.NextDouble();
    public override int Next() => (int)(_rng.NextUInt64() & 0x7FFFFFFFUL);
    public override int Next(int maxValue) => maxValue <= 0 ? 0 : _rng.NextInt(maxValue);
    public override int Next(int minValue, int maxValue) => _rng.NextInt(minValue, maxValue);
    public override long NextInt64() => (long)(_rng.NextUInt64() & 0x7FFFFFFFFFFFFFFFUL);
    public override long NextInt64(long maxValue) => maxValue <= 0 ? 0 : _rng.NextInt64(maxValue);
    public override long NextInt64(long minValue, long maxValue) => minValue >= maxValue ? minValue : _rng.NextInt64(minValue, maxValue);
    public override double NextDouble() => _rng.NextDouble();
    public override void NextBytes(byte[] buffer) => _rng.NextBytes(buffer);
    public override void NextBytes(Span<byte> buffer) => _rng.NextBytes(buffer);
}

internal sealed class ReusableRandomizer : Randomizer
{
    private readonly FastSeedableRandom _fastRandom = new();

    public ReusableRandomizer() : base(0)
    {
        localSeed = _fastRandom;
    }

    public void Reseed(ulong seed) => _fastRandom.Reseed(seed);
}

/// <summary>
/// Zero-cache Bogus column generator backed by an isolated <see cref="Faker"/> and an O(1)-reseedable
/// <see cref="ReusableRandomizer"/>. Safe for parallel per-column execution once the locale is loaded.
/// </summary>
internal sealed class BogusFallbackGenerator : IFakeColumnGenerator
{
    private readonly Faker _faker;
    private readonly ReusableRandomizer _randomizer;
    private readonly Func<Faker, object?> _generator;

    public Type NativeClrType { get; }

    public BogusFallbackGenerator(string locale, Func<Faker, object?> generator, Type nativeClrType)
    {
        _randomizer = new ReusableRandomizer();
        _faker = new Faker(locale) { Random = _randomizer };
        _generator = generator;
        NativeClrType = nativeClrType;
    }

    public IArrowArray GenerateColumn(
        Field targetField,
        int rowCount,
        ReadOnlySpan<ulong> rowKeys,
        ReadOnlySpan<bool> hasRowKey,
        bool deterministicMode,
        ulong colSalt,
        ulong globalSeed,
        long batchStartRowIndex,
        IArrowArray? skipNullSource)
    {
        if (targetField.DataType is StringType)
        {
            var builder = new StringArray.Builder();
            builder.Reserve(rowCount);

            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i))
                {
                    builder.AppendNull();
                    continue;
                }

                var cellSeed = FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex);
                _randomizer.Reseed(cellSeed);
                builder.Append(_generator(_faker)?.ToString());
            }

            return builder.Build();
        }

        var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
        var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);

        for (int i = 0; i < rowCount; i++)
        {
            if (skipNullSource is not null && skipNullSource.IsNull(i))
            {
                appender(null);
                continue;
            }

            var cellSeed = FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex);
            _randomizer.Reseed(cellSeed);
            appender(_generator(_faker));
        }

        return ArrowTypeMapper.BuildArray(genericBuilder);
    }
}
