using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Checksum;

public sealed class ChecksumDataWriter : IDataWriter, IColumnarDataWriter, IRequiresOptions<ChecksumWriterOptions>
{
	private readonly ChecksumWriterOptions _options;
	private readonly ILogger _logger;
	private readonly StringBuilder _buffer = new();
	private readonly SHA256 _hasher = SHA256.Create();
	// Uses incremental hash chaining to ensure both data integrity and row order.
	// Computation: running_hash = SHA256(running_hash + SHA256(current_row))
	private byte[] _currentHash = new byte[32]; // Start with zeroed

	public ChecksumDataWriter(string connectionString, ChecksumWriterOptions options, ILogger<ChecksumDataWriter> logger)
	{
		_options = options;
		_options.OutputPath = connectionString;
		_logger = logger;
	}

	public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_logger.IsEnabled(LogLevel.Information))
		{
			_logger.LogInformation("Initializing Checksum Writer to {Path}", ConnectionStringSanitizer.Sanitize(_options.OutputPath));
		}
		return ValueTask.CompletedTask;
	}

	public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		UpdateHash(rows);
		return ValueTask.CompletedTask;
	}

	public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		// For now, convert to rows for reuse of UpdateHash.
		// Optimization: iterate columns directly.
		var rows = ConvertRecordBatchToRows(batch);
		UpdateHash(rows);
		return ValueTask.CompletedTask;
	}

	private void UpdateHash(IReadOnlyList<object?[]> rows)
	{
		foreach (var row in rows)
		{
			// Canonicalize row to string: "val1|val2|val3"
			_buffer.Clear();
			for (int i = 0; i < row.Length; i++)
			{
				if (i > 0) _buffer.Append('|');

				var val = row[i];
				if (val == null || val == DBNull.Value)
				{
					_buffer.Append("NULL");
				}
				else if (val is DateTime dt)
				{
					// Use fixed format for consistency across providers
					_buffer.Append(dt.ToString("yyyy-MM-dd HH:mm:ss.fff"));
				}
				else if (val is double d)
				{
					// Use fixed culture
					_buffer.Append(d.ToString("G", System.Globalization.CultureInfo.InvariantCulture));
				}
				else
				{
					_buffer.Append(val.ToString());
				}
			}

			var rowString = _buffer.ToString();
			var rowBytes = Encoding.UTF8.GetBytes(rowString);

			// Hash current row
			var rowHash = SHA256.HashData(rowBytes);

			// Update running hash: running = Hash(running + rowHash)
			var combined = new byte[_currentHash.Length + rowHash.Length];
			Buffer.BlockCopy(_currentHash, 0, combined, 0, _currentHash.Length);
			Buffer.BlockCopy(rowHash, 0, combined, _currentHash.Length, rowHash.Length);

			_currentHash = SHA256.HashData(combined);
		}
	}

	private object?[][] ConvertRecordBatchToRows(RecordBatch batch)
	{
		var rows = new object?[batch.Length][];
		for (int r = 0; r < batch.Length; r++)
		{
			var row = new object?[batch.ColumnCount];
			for (int c = 0; c < batch.ColumnCount; c++)
			{
				row[c] = GetValue(batch.Column(c), r);
			}
			rows[r] = row;
		}
		return rows;
	}

	private object? GetValue(IArrowArray column, int rowIndex)
	{
		if (column.IsNull(rowIndex)) return null;

		return column switch
		{
			Int32Array a => a.GetValue(rowIndex),
			Int64Array a => a.GetValue(rowIndex),
			DoubleArray a => a.GetValue(rowIndex),
			StringArray a => a.GetString(rowIndex),
			BooleanArray a => a.GetValue(rowIndex),
			TimestampArray a => a.GetTimestamp(rowIndex),
			Date64Array a => a.GetDateTime(rowIndex),
			_ => column.ToString() // fallback
		};
	}

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		var finalHash = Convert.ToHexString(_currentHash);
		if (_logger.IsEnabled(LogLevel.Information))
		{
			_logger.LogInformation("Final Checksum: {Hash}", finalHash);
		}

		if (!string.IsNullOrEmpty(_options.OutputPath) && _options.OutputPath != "-")
		{
			var output = _options.OutputPath;
			await File.WriteAllTextAsync(output, finalHash, ct);
		}
		else
		{
			Console.WriteLine(finalHash);
		}
	}

	public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		throw new NotSupportedException("Executing raw commands is not supported for Checksum targets.");
	}

	public ValueTask DisposeAsync()
	{
		_hasher.Dispose();
		return ValueTask.CompletedTask;
	}
}
