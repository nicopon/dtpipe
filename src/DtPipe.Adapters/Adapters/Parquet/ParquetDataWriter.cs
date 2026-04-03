using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.IO;

namespace DtPipe.Adapters.Parquet;

/// <summary>
/// Writes data to Parquet format with row group streaming.
/// Supports safe dry-run inspection.
/// </summary>
public sealed class ParquetDataWriter(string outputPath) : IColumnarDataWriter, IRequiresOptions<ParquetWriterOptions>, ISchemaInspector
{
	private string _outputPath = outputPath;
	private Stream? _outputStream;

	private ParquetSchema? _schema;
	private ParquetWriter? _writer;
	private IReadOnlyList<PipeColumnInfo>? _columns;
	private DataField[]? _dataFields;

	public bool RequiresTargetInspection => false;

	public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (_outputPath == "-")
		{
			return new TargetSchemaInfo([], false, null, null, null);
		}

        if (string.IsNullOrEmpty(_outputPath))
        {
             throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

		if (!File.Exists(_outputPath))
		{
			return new TargetSchemaInfo([], false, null, null, null);
		}

		try
		{
			using var fileStream = new FileStream(_outputPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
			using var reader = await ParquetReader.CreateAsync(fileStream, cancellationToken: ct);

			var schema = reader.Schema;
			var columns = new List<TargetColumnInfo>();

			foreach (var field in schema.DataFields)
			{
				Type clrType = field.ClrNullableIfHasNullsType;

				columns.Add(new TargetColumnInfo(
					field.Name,
					clrType.Name,
					clrType,
					field.IsNullable,
					IsPrimaryKey: false,
					IsUnique: false,
					MaxLength: null,
					Precision: null,
					Scale: null
				));
			}

			long rowCount = 0;
			for (int i = 0; i < reader.RowGroupCount; i++)
			{
				using var groupReader = reader.OpenRowGroupReader(i);
				rowCount += groupReader.RowCount;
			}

			return new TargetSchemaInfo(columns, true, rowCount, fileStream.Length, null);
		}
		catch
		{
			// If file is corrupted or unreadable
			return null;
		}
	}

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        if (_outputPath == "-")
        {
            if (!Console.IsOutputRedirected)
            {
                throw new InvalidOperationException("Refusing to write binary Parquet data to the console. Redirect output with > or use a file path.");
            }
            _outputStream = Console.OpenStandardOutput();
        }
		else
		{
			// T64/T66: If it's a directory, append export.parquet
			if (Directory.Exists(_outputPath) || _outputPath.EndsWith("/") || _outputPath.EndsWith("\\"))
			{
				_outputPath = Path.Combine(_outputPath, "export.parquet");
			}
			else if (!Path.HasExtension(_outputPath))
			{
				// File without extension
				_outputPath += ".parquet";
			}

			var directory = Path.GetDirectoryName(_outputPath);
			if (!string.IsNullOrEmpty(directory))
				Directory.CreateDirectory(directory); // no-op if already exists

			_outputStream = new FileStream(_outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
		}

		_columns = columns;
		_dataFields = BuildDataFields(columns);
		_schema = new ParquetSchema(_dataFields);
		_writer = await ParquetWriter.CreateAsync(_schema, _outputStream, cancellationToken: ct);
		// _writer.CompressionMethod = CompressionMethod.Snappy;
	}

	public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (_writer is null || _dataFields is null)
			throw new InvalidOperationException("Call InitializeAsync first.");

		using (batch)
		{
			// Create row group for this batch
			using var rowGroup = _writer.CreateRowGroup();

			for (int i = 0; i < batch.ColumnCount; i++)
			{
				var arrowArray = batch.Column(i);
				var dataField = _dataFields[i];
				var dataColumn = ArrowToParquetConverter.Convert(arrowArray, dataField);
				await rowGroup.WriteColumnAsync(dataColumn, ct);
			}
		}
	}

	private static DataField[] BuildDataFields(IReadOnlyList<PipeColumnInfo> columns)
	{
		var fields = new DataField[columns.Count];

		for (var i = 0; i < columns.Count; i++)
		{
			fields[i] = MapToDataField(columns[i]);
		}

		return fields;
	}

	private static DataField MapToDataField(PipeColumnInfo col)
	{
		var baseType = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;

		return baseType switch
		{
			Type t when t == typeof(bool) => new DataField<bool?>(col.Name),
			Type t when t == typeof(byte) => new DataField<byte?>(col.Name),
			Type t when t == typeof(sbyte) => new DataField<sbyte?>(col.Name),
			Type t when t == typeof(short) => new DataField<short?>(col.Name),
			Type t when t == typeof(int) => new DataField<int?>(col.Name),
			Type t when t == typeof(long) => new DataField<long?>(col.Name),
			Type t when t == typeof(float) => new DataField<float?>(col.Name),
			Type t when t == typeof(double) => new DataField<double?>(col.Name),
			Type t when t == typeof(decimal) => new DataField<decimal?>(col.Name),
			Type t when t == typeof(DateTime) => new DataField<DateTime?>(col.Name),
			Type t when t == typeof(DateTimeOffset) => new DataField<DateTimeOffset?>(col.Name),
			Type t when t == typeof(TimeSpan) => new DataField<TimeSpan?>(col.Name),
			Type t when t == typeof(Guid) => new DataField<Guid?>(col.Name),
			Type t when t == typeof(byte[]) => new DataField<byte[]>(col.Name),
			_ => new DataField<string?>(col.Name)
		};
	}

	public ValueTask CompleteAsync(CancellationToken ct = default)
	{
		// Parquet.Net handles flushing on dispose
		return ValueTask.CompletedTask;
	}

	public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		throw new NotSupportedException("Executing raw commands is not supported for Parquet targets.");
	}

	public async ValueTask DisposeAsync()
	{
		if (_writer != null)
		{
			_writer.Dispose();
		}

		if (_outputStream != null)
		{
			await _outputStream.DisposeAsync();
		}
	}
}
