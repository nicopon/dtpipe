using System.Buffers.Binary;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A peer's length prefix never makes the node allocate past <see cref="ArrowIpcRowCounter.MaxMetadataBytes"/>: a length beyond
/// it, or below zero, is refused before anything is allocated.
/// </summary>
public class ArrowIpcRowCounterTests
{
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(ArrowIpcRowCounter.MaxMetadataBytes + 1)]
    [InlineData(-8)]
    public void A_Metadata_Length_Out_Of_Bounds_Is_Refused(int announced)
    {
        var prefix = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(prefix, 0xFFFFFFFFu);
        BinaryPrimitives.WriteInt32LittleEndian(prefix.AsSpan(4), announced);

        var refusal = Assert.Throws<InvalidDataException>(() => new ArrowIpcRowCounter().Feed(prefix));
        Assert.Contains(announced.ToString(System.Globalization.CultureInfo.InvariantCulture), refusal.Message);
    }
}
