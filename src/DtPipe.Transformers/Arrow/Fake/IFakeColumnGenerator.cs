using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Generates an entire Arrow column for one fake mapping in a single pass with zero cache.
/// </summary>
internal interface IFakeColumnGenerator
{
    Type NativeClrType { get; }

    IArrowArray GenerateColumn(
        Field targetField,
        int rowCount,
        ReadOnlySpan<ulong> rowKeys,
        ReadOnlySpan<bool> hasRowKey,
        bool deterministicMode,
        ulong colSalt,
        ulong globalSeed,
        long batchStartRowIndex,
        IArrowArray? skipNullSource);
}

internal static class FakeCellSeed
{
    private const ulong AllNullRowMix = 0xA5A5A5A5A5A5A5A5UL;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Compute(
        int rowIndexInBatch,
        ReadOnlySpan<ulong> rowKeys,
        ReadOnlySpan<bool> hasRowKey,
        bool deterministicMode,
        ulong colSalt,
        ulong globalSeed,
        long batchStartRowIndex)
    {
        if (!deterministicMode || rowKeys.IsEmpty)
            return SplitMix64.Mix64((ulong)(batchStartRowIndex + rowIndexInBatch + 1) ^ colSalt ^ globalSeed);

        if (hasRowKey[rowIndexInBatch])
            return SplitMix64.Mix64(rowKeys[rowIndexInBatch] ^ colSalt ^ globalSeed);

        return SplitMix64.Mix64((ulong)(batchStartRowIndex + rowIndexInBatch + 1) ^ colSalt ^ globalSeed ^ AllNullRowMix);
    }
}
