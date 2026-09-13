using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet;
using Parquet.Schema;
using ParquetField = Parquet.Schema.Field;

namespace DtPipe.Adapters.Parquet;

public class ParquetStreamReader : IColumnarStreamReader
{
	private readonly string _filePath;
	private readonly ILogger _logger;
	private ParquetReader? _reader;
	private readonly SemaphoreSlim _semaphore = new(1, 1);
	private FileStream? _fileStream;
	private bool _isReading;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema { get; private set; }

	public ParquetStreamReader(string filePath, ILogger? logger = null)
	{
		_filePath = filePath;
		_logger = logger ?? NullLogger.Instance;
	}

	private static async Task ValidateFileAccessAsync(string filePath, CancellationToken ct)
	{
		if (!File.Exists(filePath))
			throw new FileNotFoundException($"Parquet file not found: {filePath}", filePath);

		var fileInfo = new FileInfo(filePath);
		if (fileInfo.Length == 0)
			throw new InvalidOperationException($"Parquet file is empty: {filePath}");

		// Retry loop to handle transient locks or filesystem lag (especially on Mac/Unix)
		int retries = 5;
		while (retries > 0)
		{
			try
			{
				// Test read access with minimal I/O
				await using var testStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1, FileOptions.None);
				var buffer = new byte[1];
				_ = await testStream.ReadAsync(buffer.AsMemory(0, 1), ct);
				return;
			}
			catch (Exception ex) when (retries > 1 && (ex is IOException || ex.GetType().Name == "AccessViolationException"))
			{
				retries--;
				await Task.Delay(100, ct);
			}
			catch
			{
				if (retries <= 1) throw;
				retries--;
				await Task.Delay(100, ct);
			}
		}
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		// Validate file access before opening
		await ValidateFileAccessAsync(_filePath, ct);

		if (_logger.IsEnabled(LogLevel.Debug))
			_logger.LogDebug("Opening Parquet file: {FilePath}", _filePath);

		_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
		_reader = await ParquetReader.CreateAsync(_fileStream, leaveStreamOpen: true, cancellationToken: ct);

		var schema = _reader.Schema;
		var columns = new List<PipeColumnInfo>();

		foreach (var field in schema.Fields)
		{
			switch (field)
			{
				case DataField dataField:
				{
					var (precision, scale) = DeclaredDecimal(dataField);
					columns.Add(new PipeColumnInfo(
						dataField.Name, NormalizeClrType(dataField.ClrType), dataField.IsNullable,
						Precision: precision, Scale: scale));
					break;
				}

				case ListField listField:
					columns.Add(new PipeColumnInfo(listField.Name, ListClrType(listField), IsNullable: true));
					break;

				// Dropping it silently would leave Columns and the read loop disagreeing on how
				// many columns the file has, which is the shape the list bug took: the schema lost
				// a column while the loop found an extra one.
				default:
					throw new NotSupportedException(
						$"Parquet column '{field.Name}' is a {field.GetType().Name}, which this reader " +
						"cannot represent. Project it in your query, for example to JSON text.");
			}
		}

		Columns = columns;
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		await _semaphore.WaitAsync(ct);
		try
		{
			if (_isReading)
				throw new InvalidOperationException("Concurrent reads from the same ParquetStreamReader are not supported.");
			_isReading = true;
		}
		finally
		{
			_semaphore.Release();
		}

		try
		{
			var schema = DtPipe.Core.Infrastructure.Arrow.ArrowSchemaFactory.Create(Columns);

			for (int rowGroupIndex = 0; rowGroupIndex < _reader.RowGroupCount; rowGroupIndex++)
			{
				ct.ThrowIfCancellationRequested();

				using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
				var rowCount = (int)rowGroupReader.RowCount;
				var arrays = new List<IArrowArray>();

				// Top-level fields, not Schema.DataFields: the latter flattens a LIST onto its leaf,
				// which carries one entry per element rather than per row.
				foreach (var field in _reader.Schema.Fields)
				{
					var columnData = await ReadColumnAsRowValuesAsync(rowGroupReader, field, ct);
					var (clrType, precision, scale) = ColumnShape(field);
					arrays.Add(DtPipe.Core.Infrastructure.Arrow.ArrowArrayFactory.Create(
						columnData, clrType, IsFieldNullable(field), precision, scale));
				}

				yield return new RecordBatch(schema, arrays, rowCount);
			}
		}
		finally
		{
			_isReading = false;
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		// Mark as reading to prevent concurrent reads
		await _semaphore.WaitAsync(ct);
		try
		{
			if (_isReading)
				throw new InvalidOperationException("Concurrent reads from the same ParquetStreamReader are not supported.");
			_isReading = true;
		}
		finally
		{
			_semaphore.Release();
		}

		try
		{
			var batch = new object?[batchSize][];
			var index = 0;

			// Read all row groups
			for (int rowGroupIndex = 0; rowGroupIndex < _reader.RowGroupCount; rowGroupIndex++)
			{
				ct.ThrowIfCancellationRequested();

				// Check if reader was disposed
				await _semaphore.WaitAsync(ct);
				try
				{
					if (_reader == null) yield break;
				}
				finally
				{
					_semaphore.Release();
				}

				using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
				var rowCount = (int)rowGroupReader.RowCount;

				// Read all columns for this row group
				var columnDataArrays = new System.Array[Columns.Count];
				for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
				{
					columnDataArrays[colIndex] =
						await ReadColumnAsRowValuesAsync(rowGroupReader, _reader.Schema.Fields[colIndex], ct);
				}

				// Yield rows
				for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
				{
					ct.ThrowIfCancellationRequested();

					var row = new object?[Columns.Count];
					for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
					{
						row[colIndex] = columnDataArrays[colIndex].GetValue(rowIndex);
					}

					batch[index++] = row;

					if (index >= batchSize)
					{
						yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
						batch = new object?[batchSize][];
						index = 0;
					}
				}
			}

			if (index > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
			}
		}
		finally
		{
			_isReading = false;
		}
	}

	/// <summary>
	/// One column of a row group as one CLR value per row — a list column giving an array per row,
	/// so both read paths index by row and neither has to know how the file nests.
	/// </summary>
	private static Task<System.Array> ReadColumnAsRowValuesAsync(
		ParquetRowGroupReader rowGroupReader, ParquetField field, CancellationToken ct)
		=> field switch
		{
			ListField list => ReadListColumnAsync(rowGroupReader, list, (int)rowGroupReader.RowCount, ct),
			DataField data => ReadColumnDataAsArrayAsync(rowGroupReader, data, ct),
			_ => throw new NotSupportedException($"Parquet column '{field.Name}' is a {field.GetType().Name}."),
		};

	private static (Type ClrType, int? Precision, int? Scale) ColumnShape(ParquetField field)
	{
		if (field is ListField list) return (ListClrType(list), null, null);

		var data = (DataField)field;
		var (precision, scale) = DeclaredDecimal(data);
		return (NormalizeClrType(data.ClrType), precision, scale);
	}

	private static bool IsFieldNullable(ParquetField field)
		=> field is not DataField data || data.IsNullable;

	/// <summary>
	/// The CLR type standing for a list column: an array of the item type, made nullable when the
	/// item is, so a NULL element keeps its slot instead of collapsing to the type's default.
	/// </summary>
	private static Type ListClrType(ListField list)
	{
		var item = (DataField)list.Item;
		var element = NormalizeClrType(item.ClrType);

		if (item.IsNullable && element.IsValueType && Nullable.GetUnderlyingType(element) is null)
			element = typeof(Nullable<>).MakeGenericType(element);

		return element.MakeArrayType();
	}

	/// <summary>
	/// Reads a Parquet LIST back into one array per row, decoding the three-level encoding
	/// <c>ArrowToParquetConverter.WriteListColumnAsync</c> writes — definition 0 for a NULL list,
	/// 1 for an empty one, <c>MaxDefinitionLevel</c> for an element with a value and one below it
	/// for a NULL element; repetition 0 opens a row and 1 continues the current list.
	/// </summary>
	/// <remarks>
	/// The reader had no list support at all: <c>Schema.DataFields</c> flattens a LIST onto its
	/// leaf, which holds one entry per element, and the buffer was sized by row count — so dtpipe
	/// could not read back a single list file it had written itself. The column chunk's NumValues
	/// counts level entries, which is the upper bound for values too, so one read fills all three
	/// buffers.
	/// </remarks>
	private static Task<System.Array> ReadListColumnAsync(
		ParquetRowGroupReader rowGroupReader, ListField listField, int rowCount, CancellationToken ct)
	{
		var item = (DataField)listField.Item;

		return NormalizeClrType(item.ClrType) switch
		{
			var t when t == typeof(bool) => ReadTypedListAsync<bool>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(short) => ReadTypedListAsync<short>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(int) => ReadTypedListAsync<int>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(long) => ReadTypedListAsync<long>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(float) => ReadTypedListAsync<float>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(double) => ReadTypedListAsync<double>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(decimal) => ReadTypedListAsync<decimal>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(DateTime) => ReadTypedListAsync<DateTime>(rowGroupReader, item, rowCount, ct),
			var t when t == typeof(DateTimeOffset) => ReadTypedListAsync<DateTimeOffset>(rowGroupReader, item, rowCount, ct),

			// Parquet.Net carries repetition levels only through the value-type overloads, which is
			// the same limit that makes the writer refuse a list of text — so the two halves refuse
			// the same shape rather than one writing what the other cannot read.
			var t when t == typeof(string) => throw new NotSupportedException(
				$"Parquet column '{item.Path}': lists of text are not supported yet. " +
				"Read it through DuckDB instead, for example with --sql over read_parquet."),

			var t => throw new NotSupportedException(
				$"Parquet column '{item.Path}': a list of {t.Name} is not supported."),
		};
	}

	private static async Task<System.Array> ReadTypedListAsync<T>(
		ParquetRowGroupReader rowGroupReader, DataField item, int rowCount, CancellationToken ct)
		where T : struct
	{
		var rows = new T?[]?[rowCount];
		var levelCount = (int)(rowGroupReader.GetMetadata(item)?.MetaData?.NumValues ?? 0);
		if (levelCount == 0) return rows;

		var definitions = new int[levelCount];
		var repetitions = new int[levelCount];
		var values = new T[levelCount];
		await rowGroupReader.ReadRawAsync<T>(
			item, values.AsMemory(), definitions.AsMemory(), repetitions.AsMemory(), ct);

		var maxDefinition = item.MaxDefinitionLevel;
		var current = new List<T?>();
		var row = -1;
		var value = 0;

		// A NULL list and an empty one are both falsy in the output array, so "is this row set yet"
		// cannot be read off rows[row] — it is tracked here. Collapsing the two is the classic
		// definition-level mistake, and the writer's own tests assert they stay distinct.
		var currentIsNull = false;

		for (var i = 0; i < levelCount; i++)
		{
			if (repetitions[i] == 0)
			{
				if (row >= 0 && row < rowCount) rows[row] = currentIsNull ? null : current.ToArray();
				current = new List<T?>();
				currentIsNull = false;
				row++;
			}

			if (row >= rowCount) break;

			if (definitions[i] == 0)
			{
				currentIsNull = true;       // the list itself is NULL, distinct from an empty one
			}
			else if (definitions[i] == maxDefinition)
			{
				current.Add(values[value++]);
			}
			else if (item.IsNullable && definitions[i] == maxDefinition - 1)
			{
				current.Add(null);          // the list holds a NULL element
			}
			// else: definition below the element levels — the list is present and empty.
		}

		if (row >= 0 && row < rowCount) rows[row] = currentIsNull ? null : current.ToArray();

		return rows;
	}

 	private static async Task<System.Array> ReadColumnDataAsArrayAsync(ParquetRowGroupReader rowGroupReader, DataField field, CancellationToken ct)
 		{
 		int rowCount = (int)rowGroupReader.RowCount;
 		Type baseType = NormalizeClrType(field.ClrType);

		if (baseType == typeof(string))
		{
			var data = new string?[rowCount];
			await rowGroupReader.ReadAsync(field, data.AsMemory(), null, ct);
			return data;
		}
		if (baseType == typeof(byte[]))
		{
			var data = new ReadOnlyMemory<byte>?[rowCount];
			await rowGroupReader.ReadAsync<ReadOnlyMemory<byte>>(field, data.AsMemory(), null, ct);
			var result = new byte[rowCount][];
			for (int i = 0; i < rowCount; i++)
				result[i] = data[i]?.ToArray()!;
			return result;
		}

		// Other types use the generic ReadAsync<T>(..., Memory<T?>, ...)
		return baseType switch
		{
			_ when baseType == typeof(bool) => await ReadTypedAsync<bool>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(byte) => await ReadTypedAsync<byte>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(sbyte) => await ReadTypedAsync<sbyte>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(short) => await ReadTypedAsync<short>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(int) => await ReadTypedAsync<int>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(long) => await ReadTypedAsync<long>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(float) => await ReadTypedAsync<float>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(double) => await ReadTypedAsync<double>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(decimal) => await ReadTypedAsync<decimal>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(DateTime) => await ReadTypedAsync<DateTime>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(DateTimeOffset) => await ReadTypedAsync<DateTimeOffset>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(TimeSpan) => await ReadTypedAsync<TimeSpan>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(Guid) => await ReadTypedAsync<Guid>(rowGroupReader, field, rowCount, ct),
			_ => throw new NotSupportedException($"Unsupported Parquet type: {baseType.Name}")
		};
	}


 	private static async Task<T?[]> ReadTypedAsync<T>(ParquetRowGroupReader rowGroupReader, DataField field, int rowCount, CancellationToken ct)
 		where T : struct
 		{
 		var data = new T?[rowCount];
 		await rowGroupReader.ReadAsync<T>(field, data, cancellationToken: ct);
 		return data;
 		}

 	/// <summary>
 	/// Precision and scale a Parquet decimal column declares, so the Arrow decimal built from it
 	/// keeps the source's width. A file that declares none leaves both null.
 	/// </summary>
 	private static (int? Precision, int? Scale) DeclaredDecimal(DataField field)
 		=> field is DecimalDataField dec ? (dec.Precision, dec.Scale) : (null, null);

 	// Parquet.Net 6.1.0 reports string columns as ReadOnlyMemory<char> and binary columns as
 	// ReadOnlyMemory<byte> in DataField.ClrType instead of string / byte[]. Map them back to the
 	// canonical DtPipe CLR types so schema creation and the type-dispatch switch below behave as before.
 	internal static Type NormalizeClrType(Type clrType)
 			{
 		var type = Nullable.GetUnderlyingType(clrType) ?? clrType;
 		if (type == typeof(ReadOnlyMemory<char>))
 			return typeof(string);
 		if (type == typeof(ReadOnlyMemory<byte>))
 			return typeof(byte[]);
 		return type;
 			}

	public async ValueTask DisposeAsync()
	{
		await _semaphore.WaitAsync();
		try
		{
			if (_reader != null)
			{
				await _reader.DisposeAsync();
				_reader = null;
			}
			if (_fileStream != null)
			{
				await _fileStream.DisposeAsync();
				_fileStream = null;
			}
		}
		finally
		{
			_semaphore.Release();
			_semaphore.Dispose();
		}
	}
}
