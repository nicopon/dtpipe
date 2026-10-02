using System.Buffers.Binary;

namespace DtPipe.PipelineNode;

/// <summary>
/// Counts the rows of an Arrow IPC stream from its framing alone — the continuation marker, the
/// metadata length, and the <c>RecordBatch</c> message's own <c>length</c> field — without ever
/// decoding a column buffer. Fed arbitrary byte chunks; a message's boundary is never assumed to
/// line up with a chunk boundary, since the node relays bytes at whatever size it read them.
/// </summary>
/// <remarks>
/// The FlatBuffer vtable offsets below are Arrow's own wire format: <c>Message.HeaderType</c> at
/// vtable byte 6, <c>Message.Header</c> at 8, <c>Message.BodyLength</c> at 10, and — inside the
/// header union when it is a <c>RecordBatch</c> — <c>RecordBatch.Length</c> at vtable byte 4. A
/// <c>DictionaryBatch</c> (header type 2) carries its own, differently-meaning <c>length</c> field
/// and is deliberately never added to <see cref="RowCount"/>.
/// </remarks>
public sealed class ArrowIpcRowCounter
{
    private const uint ContinuationMarker = 0xFFFFFFFFu;
    private const byte RecordBatchHeaderType = 3;

    private enum State { Continuation, MetadataLength, Metadata, Body, Done }

    private State _state = State.Continuation;
    private readonly byte[] _small = new byte[4];
    private int _smallFilled;
    private byte[]? _metadata;
    private int _metadataFilled;
    private long _bodyRemaining;

    public long RowCount { get; private set; }

    /// <summary>True once the zero-length metadata marker that ends the stream has been seen.</summary>
    public bool SawEndOfStream { get; private set; }

    /// <summary>
    /// Consumes as much of <paramref name="data"/> as the current framing state allows. Running out
    /// of input mid-message is not an error — a killed producer is expected to do exactly that, and
    /// <see cref="RowCount"/>/<see cref="SawEndOfStream"/> simply report what was seen so far.
    /// </summary>
    public void Feed(ReadOnlySpan<byte> data)
    {
        while (data.Length > 0 && _state != State.Done)
        {
            switch (_state)
            {
                case State.Continuation:
                    data = FillSmall(data);
                    if (_smallFilled == 4)
                    {
                        uint marker = BinaryPrimitives.ReadUInt32LittleEndian(_small);
                        if (marker != ContinuationMarker)
                            throw new InvalidDataException(
                                $"Arrow IPC stream: expected the continuation marker (0x{ContinuationMarker:X8}), found 0x{marker:X8}.");
                        _smallFilled = 0;
                        _state = State.MetadataLength;
                    }
                    break;

                case State.MetadataLength:
                    data = FillSmall(data);
                    if (_smallFilled == 4)
                    {
                        int metadataLength = BinaryPrimitives.ReadInt32LittleEndian(_small);
                        _smallFilled = 0;
                        if (metadataLength == 0)
                        {
                            SawEndOfStream = true;
                            _state = State.Done;
                        }
                        else
                        {
                            _metadata = new byte[metadataLength];
                            _metadataFilled = 0;
                            _state = State.Metadata;
                        }
                    }
                    break;

                case State.Metadata:
                {
                    var meta = _metadata!;
                    int take = Math.Min(meta.Length - _metadataFilled, data.Length);
                    data[..take].CopyTo(meta.AsSpan(_metadataFilled));
                    _metadataFilled += take;
                    data = data[take..];
                    if (_metadataFilled == meta.Length)
                    {
                        ParseMessageMetadata(meta);
                        _metadata = null;
                        _state = _bodyRemaining > 0 ? State.Body : State.Continuation;
                    }
                    break;
                }

                case State.Body:
                {
                    long take = Math.Min(_bodyRemaining, data.Length);
                    data = data[(int)take..];
                    _bodyRemaining -= take;
                    if (_bodyRemaining == 0)
                        _state = State.Continuation;
                    break;
                }
            }
        }
    }

    private ReadOnlySpan<byte> FillSmall(ReadOnlySpan<byte> data)
    {
        int take = Math.Min(4 - _smallFilled, data.Length);
        data[..take].CopyTo(_small.AsSpan(_smallFilled));
        _smallFilled += take;
        return data[take..];
    }

    private void ParseMessageMetadata(byte[] metadata)
    {
        int tablePos = ReadInt32(metadata, 0);
        int vtablePos = tablePos - ReadInt32(metadata, tablePos);
        int vtableSize = ReadUInt16(metadata, vtablePos);

        byte headerType = 0;
        if (6 < vtableSize)
        {
            int fieldRel = ReadUInt16(metadata, vtablePos + 6);
            if (fieldRel != 0)
                headerType = metadata[tablePos + fieldRel];
        }

        long bodyLength = 0;
        if (10 < vtableSize)
        {
            int fieldRel = ReadUInt16(metadata, vtablePos + 10);
            if (fieldRel != 0)
                bodyLength = ReadInt64(metadata, tablePos + fieldRel);
        }
        _bodyRemaining = bodyLength;

        if (headerType != RecordBatchHeaderType || 8 >= vtableSize) return;

        int headerFieldRel = ReadUInt16(metadata, vtablePos + 8);
        if (headerFieldRel == 0) return;

        int headerFieldPos = tablePos + headerFieldRel;
        int rbTablePos = headerFieldPos + ReadInt32(metadata, headerFieldPos);
        int rbVtablePos = rbTablePos - ReadInt32(metadata, rbTablePos);
        int rbVtableSize = ReadUInt16(metadata, rbVtablePos);
        if (4 >= rbVtableSize) return;

        int lenFieldRel = ReadUInt16(metadata, rbVtablePos + 4);
        if (lenFieldRel == 0) return;

        RowCount += ReadInt64(metadata, rbTablePos + lenFieldRel);
    }

    private static int ReadInt32(byte[] b, int pos) => BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(pos, 4));
    private static ushort ReadUInt16(byte[] b, int pos) => BinaryPrimitives.ReadUInt16LittleEndian(b.AsSpan(pos, 2));
    private static long ReadInt64(byte[] b, int pos) => BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(pos, 8));
}
