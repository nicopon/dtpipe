namespace DtPipe.Adapters.Arrow;

/// <summary>
/// Records whether the wrapped stream ran out of bytes while the Arrow reader was still reading,
/// which is what tells a truncated IPC stream apart from a complete one.
/// </summary>
/// <remarks>
/// An Arrow IPC stream ends with the marker <c>ffffffff 00000000</c>. A producer killed between
/// two messages leaves a stream that stops on a clean message boundary, and
/// <c>ArrowStreamReader.ReadNextRecordBatchAsync</c> answers <c>null</c> for that exactly as it
/// does for a stream that carried its marker. A consumer that only tests for <c>null</c> therefore
/// writes what arrived, exits 0 and says nothing: 100 000 rows of 400 000 were lost that way, and
/// a reader that goes back to testing <c>null</c> alone loses them again.
///
/// The distinction is visible one level down. Reading the marker consumes eight bytes that are
/// there, so a complete stream never makes the source report end-of-file, while a stream that
/// stopped early always does. This probe records that one fact and judges nothing.
///
/// It rests on the reader not reading past the marker — a property of the Arrow version in use,
/// not of the format. <c>ArrowStreamTruncationTests</c> pins it: should a future version read
/// ahead, complete streams start being refused, which is loud. That is the only direction this
/// defect is allowed to fail in.
/// </remarks>
internal sealed class ArrowStreamTerminationProbe(Stream inner) : Stream
{
	/// <summary>True once the source reported end-of-file to a read that asked for bytes.</summary>
	public bool SourceRanDry { get; private set; }

	private int Observe(int requested, int received)
	{
		if (requested > 0 && received == 0) SourceRanDry = true;
		return received;
	}

	public override int Read(byte[] buffer, int offset, int count)
		=> Observe(count, inner.Read(buffer, offset, count));

	public override int Read(Span<byte> buffer)
		=> Observe(buffer.Length, inner.Read(buffer));

	public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
		=> Observe(count, await inner.ReadAsync(buffer.AsMemory(offset, count), ct));

	public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
		=> Observe(buffer.Length, await inner.ReadAsync(buffer, ct));

	public override bool CanRead => inner.CanRead;
	public override bool CanSeek => inner.CanSeek;
	public override bool CanWrite => false;
	public override long Length => inner.Length;
	public override long Position { get => inner.Position; set => inner.Position = value; }
	public override void Flush() => inner.Flush();
	public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
	public override void SetLength(long value) => throw new NotSupportedException();
	public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
