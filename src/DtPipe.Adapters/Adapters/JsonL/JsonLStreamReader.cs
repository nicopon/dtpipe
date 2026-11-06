using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Adapters.Common;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;
using System.Collections;
using DtPipe.Core.Infrastructure.Discovery;

namespace DtPipe.Adapters.JsonL;

public class JsonLStreamReader : IStreamReader, IColumnarStreamReader, IColumnTypeInferenceCapable
{
    private readonly string _filePath;
    private readonly JsonLReaderOptions _options;
    private readonly ILogger _logger;

    private FileStream? _fileStream;
    private StreamReader? _streamReader;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    public JsonLStreamReader(string filePath, JsonLReaderOptions options, ILogger? logger = null)
    {
        _filePath = filePath;
        _options = options;
        _logger = logger ?? NullLogger.Instance;
        _isResetSupported = !string.IsNullOrEmpty(_filePath) && _filePath != "-";
    }

    private bool _isResetSupported;
    // Records consumed during schema inference are buffered here and replayed during data extraction.
    // This enables single-pass processing even for large files where Reset() would be expensive.
    // Each entry keeps where its record stood (see _position), so a replayed record is named as a fresh read names it.
    private readonly List<(object Raw, long Position)> _inferenceBuffer = new();

    // --path only: held across the inference pass and the real pass instead of re-navigating from a
    // cold reader each time, which would resume wherever the first pass left the stream (end of file
    // for a small array, mid-array for a large one) rather than at the start of the target array.
    private IAsyncEnumerator<JsonElement>? _pathElementEnumerator;
    private readonly CancellationTokenSource _pathElementsCts = new();
    private int _bufferPos = 0;
    private bool _isBufferingActive = true;

    private IReadOnlyDictionary<string, string>? _autoAppliedTypes;
    public IReadOnlyDictionary<string, string>? AutoAppliedTypes => _autoAppliedTypes;
    public int MaxSample => _options.MaxSample;

    // Set when the columns come from the data or from a declared Arrow schema: a record with a key the
    // schema has no column for is then refused, never written without it. --column-types is a projection.
    private JsonLRecordShape? _shape;
    private bool _shapeIsDeclared;
    private HashSet<string>? _textColumns;

    // The columns whose values are read into their type here, so the Arrow serializer and a row consumer
    // both receive that type, and a value it does not hold stops the run at its cell. The type is the
    // one --column-types declares or, without it, the one the sample gave (_typesInferred); a replayed
    // sample record is read through the same readers as a fresh one. A struct or list column the
    // sample gave reads each field and item the same way (Nested).
    private Dictionary<string, TypedColumn>? _typedColumns;
    private bool _typesInferred;
    private readonly record struct TypedColumn(string Name, Type Type, Func<JsonElement, object?> Read, JsonLNestedReader? Nested = null);

    // Where the record being read stands: its line in the file (blank lines counted), or its rank in
    // the array under --path.
    private long _position;

    private void UpdateTextColumns()
    {
        if (Columns == null)
        {
            _textColumns = null;
            return;
        }

        _textColumns = new HashSet<string>(
            Columns.Where(c => c.ClrType == typeof(string)).Select(c => c.Name),
            StringComparer.OrdinalIgnoreCase);
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        if (!string.IsNullOrWhiteSpace(_options.Schema))
        {
            // Highest priority: full Arrow schema provided (from --schema-load or --export-job YAML).
            // Captures complete structure including nested StructType / ListType.
            BuildSchemaFromArrowJson(_options.Schema);
            _shape = JsonLRecordShape.Of(Schema!);
            _shapeIsDeclared = true;
            await ResetReaderAsync();
        }
        else if (!string.IsNullOrWhiteSpace(_options.ColumnTypes))
        {
            // Flat scalar type overrides declared by the user via --json-column-types.
            BuildSchemaFromColumnTypes();
            await ResetReaderAsync();
        }
        else
        {
            if (_options.MaxSample <= 0)
            {
                throw new InvalidOperationException(
                    $"JSONL: '--max-sample {_options.MaxSample}' leaves no record to infer the schema from, and no schema was declared. " +
                    "Declare the columns with '--schema-load' or '--column-types', or give a sample of 1 or more.");
            }

            await ResetReaderAsync();
            await InferSchemaAsync(ct);
            await ResetReaderAsync();
            _shape = JsonLRecordShape.Of(Schema!);
        }

        _isBufferingActive = false; // Stop adding new records to buffer, but keep what we have.
        _bufferPos = 0; // Prepare to drain from start
    }

    private void BuildSchemaFromArrowJson(string schemaJson)
    {
        var arrowSchema = DtPipe.Core.Infrastructure.Arrow.ArrowSchemaSerializer.Deserialize(schemaJson);
        Schema = arrowSchema;
        // Map complex types (List, Struct, Map) to typeof(object) to match the inference path.
        // This ensures ArrowSchemaFactory.Create(Columns) doesn't fail on unsupported CLR types.
        Columns = arrowSchema.FieldsList
            .Select(f =>
            {
                var clrType = GetSimplifiedClrType(f);
                var (precision, scale) = clrType == typeof(decimal) ? ArrowSchemaFactory.DeclaredWidth(f) : (null, null);
                return new PipeColumnInfo(f.Name, clrType, f.IsNullable, Precision: precision, Scale: scale);
            })
            .ToList();
        UpdateTextColumns();
        var typed = new Dictionary<string, TypedColumn>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < Columns.Count; i++)
        {
            var column = Columns[i];
            if (JsonLTypedValue.BuildReader(column.ClrType, DecimalWidth.Of(column.Precision, column.Scale)) is { } read)
                typed[column.Name] = new TypedColumn(column.Name, column.ClrType, read);
            else if (JsonLNestedReader.Of(arrowSchema.FieldsList[i]) is { } nested)
                typed[column.Name] = new TypedColumn(column.Name, column.ClrType, nested.Read, nested);
        }
        _typedColumns = typed.Count > 0 ? typed : null;
        _logger.LogInformation("JsonLStreamReader: Schema loaded from JSON ({Count} fields). Path: {Path}",
            arrowSchema.FieldsList.Count, _options.Path ?? "(root)");
    }

    private async Task ResetReaderAsync()
    {
        _bufferPos = 0; // Reset buffer pointer to allow re-reading from start of buffer

        if (!_isResetSupported)
        {
            // STDIN: We cannot reset. If we already have a stream, we keep it.
            if (_streamReader == null)
            {
                var encoding = Encoding.GetEncoding(_options.Encoding);
                _streamReader = new StreamReader(Console.OpenStandardInput(), encoding);
            }
            return;
        }

        // File-based: If we already have a reader but haven't buffered anything yet,
        // OR if we specifically want to discard everything and start over from disk,
        // we close and re-open.
        if (_inferenceBuffer.Count == 0)
        {
            if (_streamReader != null)
            {
                _streamReader.Dispose();
                _streamReader = null;
            }

            if (_fileStream != null)
            {
                await _fileStream.DisposeAsync();
                _fileStream = null;
            }

            var encoding2 = Encoding.GetEncoding(_options.Encoding);
            _fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
            _streamReader = new StreamReader(_fileStream, encoding2, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
        }
    }

    private async Task InferSchemaAsync(CancellationToken ct)
    {
        if (_streamReader == null) throw new InvalidOperationException("StreamReader is null.");

        Dictionary<string, object?>? merged = null;
        int count = 0;

        await foreach (var record in EnumerateRawRecordsAsync(ct))
        {
            if (record is Dictionary<string, object?> current)
            {
                merged ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                SchemaDiscoveryHelper.DeepMergeSchemas(merged, current);
            }

            count++;
            if (count >= _options.MaxSample) break;
        }

        if (merged == null)
        {
            Columns = System.Array.Empty<PipeColumnInfo>();
            Schema = new Schema(Enumerable.Empty<Field>(), null);
            return;
        }

        var columns = new List<PipeColumnInfo>();
        var fields = new List<Field>();
        var autoApplied = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var typed = new Dictionary<string, TypedColumn>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in merged)
        {
            var (type, arrowType) = InferTypes(kvp.Key, kvp.Value);
            columns.Add(new PipeColumnInfo(kvp.Key, type, true));
            var inferred = InferredField(kvp.Key, arrowType);
            fields.Add(inferred);
            if (JsonLTypedValue.BuildReader(type) is { } read)
                typed[kvp.Key] = new TypedColumn(kvp.Key, type, read);
            else if (JsonLNestedReader.Of(inferred) is { } nested)
                typed[kvp.Key] = new TypedColumn(kvp.Key, type, nested.Read, nested);

            var hint = DtPipe.Core.Helpers.TypeHelper.HintFor(type);
            if (hint != null) autoApplied[kvp.Key] = hint;
        }

        _autoAppliedTypes = autoApplied;
        Columns = columns;
        Schema = new Schema(fields, null);
        UpdateTextColumns();
        _typedColumns = typed.Count > 0 ? typed : null;
        _typesInferred = true;
        _logger.LogInformation("JsonLStreamReader: Inferred schema with {Count} columns. Path: {Path}", columns.Count, _options.Path ?? "(root)");
    }

    private void BuildSchemaFromColumnTypes()
    {
        var overrides = ParseColumnTypesOption(_options.ColumnTypes);
        var columns = new List<PipeColumnInfo>();
        var fields = new List<Field>();
        var typed = new Dictionary<string, TypedColumn>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in overrides)
        {
            var clrType = DtPipe.Core.Helpers.TypeHelper.RequireTypeHint("--column-types", kvp.Key, kvp.Value);
            var arrowResult = ArrowTypeMapper.GetLogicalType(clrType);
            columns.Add(new PipeColumnInfo(kvp.Key, clrType, true));
            if (JsonLTypedValue.BuildReader(clrType) is { } read)
                typed[kvp.Key] = new TypedColumn(kvp.Key, clrType, read);
            if (arrowResult.Metadata != null)
                fields.Add(new Field(kvp.Key, arrowResult.ArrowType, true, arrowResult.Metadata));
            else
                fields.Add(new Field(kvp.Key, arrowResult.ArrowType, true));
        }

        Columns = columns;
        Schema = new Schema(fields, null);
        UpdateTextColumns();
        _typedColumns = typed.Count > 0 ? typed : null;
        _logger.LogInformation("JsonLStreamReader: Schema built from ColumnTypes with {Count} columns. Path: {Path}", columns.Count, _options.Path ?? "(root)");
    }

    /// <summary>
    /// Maps an Arrow field to its simplified CLR type, using typeof(object) for complex types
    /// (ListType, StructType, MapType) to match the inference path. This prevents ArrowSchemaFactory
    /// from failing when building a schema from PipeColumnInfo with complex column types.
    /// </summary>
    private static Type GetSimplifiedClrType(Field f)
    {
        return f.DataType switch
        {
            ListType or LargeListType or StructType or MapType => typeof(object),
            _ => ArrowTypeMapper.GetClrTypeFromField(f)
        };
    }

    // ── IColumnTypeInferenceCapable ───────────────────────────────────────────

    public async Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(
        int sampleRows, CancellationToken ct = default)
    {
        if (!_isResetSupported)
            return new Dictionary<string, string>(); // STDIN cannot be re-read

        Dictionary<string, object?>? merged = null;
        int count = 0;
        var encoding = Encoding.GetEncoding(_options.Encoding);

        if (string.IsNullOrEmpty(_options.Path))
        {
            // Classic JSONL: read N lines from a fresh stream
            await using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 65536, FileOptions.Asynchronous);
            using var sr = new StreamReader(fs, encoding);

            while (count < sampleRows && !ct.IsCancellationRequested)
            {
                var line = await sr.ReadLineAsync(ct);
                if (line == null) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                var dict = ParseLineToDictionary(line);
                merged ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                SchemaDiscoveryHelper.DeepMergeSchemas(merged, dict);
                count++;
            }
        }
        else
        {
            // Hierarchical JSON: stream elements via JsonStreamingPathReader
            var pathParts = _options.Path.Split('.', StringSplitOptions.RemoveEmptyEntries);
            await using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                bufferSize: 65536, FileOptions.Asynchronous);

            await foreach (var element in JsonStreamingPathReader.StreamArrayAsync(fs, pathParts, ct))
            {
                var converted = ToValue(element);
                if (converted is Dictionary<string, object?> d)
                {
                    merged ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    SchemaDiscoveryHelper.DeepMergeSchemas(merged, d);
                }
                count++;
                if (count >= sampleRows) break;
            }
        }

        if (merged == null) return new Dictionary<string, string>();

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in merged)
        {
            var (clrType, _) = InferTypes(kvp.Key, kvp.Value);
            var hint = DtPipe.Core.Helpers.TypeHelper.HintFor(clrType);
            if (hint != null) result[kvp.Key] = hint;
        }
        return result;
    }

    // Raw material only: a line's text (classic) or a standalone JsonElement (hierarchical). Replaying
    // a buffered sample record through the same builder a fresh read uses — rather than keeping the
    // dict built while buffering — means its text columns are never decided before the schema is: the
    // schema (and _textColumns) is known only once InferSchemaAsync returns, after the buffer fills.
    private async IAsyncEnumerable<object?> EnumerateRawRecordsAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (_streamReader == null) yield break;

        bool isHierarchical = !string.IsNullOrEmpty(_options.Path);

        // 1. DRAIN the buffer first (if any), each record standing where it stood when it was read
        var streamPosition = _position;
        while (_bufferPos < _inferenceBuffer.Count)
        {
            if (ct.IsCancellationRequested) yield break;
            var (raw, position) = _inferenceBuffer[_bufferPos++];
            _position = position;
            yield return isHierarchical ? ToRecord((JsonElement)raw) : ParseLineToDictionary((string)raw, _typedColumns);
        }
        _position = streamPosition;

        // 2. Read new records from the stream
        if (!isHierarchical)
        {
            // Classic JSONL: 1 line = 1 record
            while (!ct.IsCancellationRequested)
            {
                var line = await _streamReader.ReadLineAsync(ct);
                if (line == null) break;
                _position++;
                if (string.IsNullOrWhiteSpace(line)) continue;

                if (_isBufferingActive) _inferenceBuffer.Add((line, _position));
                _bufferPos++;
                yield return ParseLineToDictionary(line, _typedColumns);
            }
        }
        else
        {
            // Hierarchical JSON: stream elements from the target path array, via an enumerator kept
            // alive on the reader (see _pathElementEnumerator) so a consumer that stops early (the
            // inference pass) leaves it positioned for the next one (the real pass) to continue from.
            var pathParts = _options.Path.Split('.', StringSplitOptions.RemoveEmptyEntries);
            _pathElementEnumerator ??= _isResetSupported
                ? JsonStreamingPathReader.StreamArrayAsync(_fileStream!, pathParts, _pathElementsCts.Token).GetAsyncEnumerator()
                : EnumeratePathElementsFromStdinAsync(pathParts, _pathElementsCts.Token).GetAsyncEnumerator();

            while (!ct.IsCancellationRequested && await _pathElementEnumerator.MoveNextAsync())
            {
                var element = _pathElementEnumerator.Current;
                // Already a standalone clone (JsonStreamingPathReader, or Clone() below), safe to buffer as is.
                _position++;
                if (_isBufferingActive) _inferenceBuffer.Add((element, _position));
                _bufferPos++;
                yield return ToRecord(element);
            }
        }
    }

    // STDIN: the whole input is loaded once here; later calls keep draining the same in-memory
    // document instead of calling ReadToEndAsync again on a stream the first call already emptied.
    private async IAsyncEnumerable<JsonElement> EnumeratePathElementsFromStdinAsync(
        string[] pathParts, [EnumeratorCancellation] CancellationToken ct)
    {
        var fullJson = await _streamReader!.ReadToEndAsync(ct);
        if (string.IsNullOrEmpty(fullJson)) yield break;

        using var doc = JsonDocument.Parse(fullJson);
        var current = doc.RootElement;
        foreach (var part in pathParts)
        {
            if (!current.TryGetProperty(part, out var next)) yield break;
            current = next;
        }

        if (current.ValueKind != JsonValueKind.Array) yield break;

        foreach (var item in current.EnumerateArray())
        {
            yield return item.Clone();
        }
    }

    private Dictionary<string, object?> ToRecord(JsonElement element)
    {
        var converted = ToRecordValue(element);
        return converted as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = converted };
    }

    private (Type ClrType, IArrowType ArrowType) InferTypes(string key, object? value)
    {
        return value switch
        {
            long => (typeof(long), Int64Type.Default),
            double => (typeof(double), DoubleType.Default),
            decimal => (typeof(decimal), ArrowTypeMap.DefaultDecimalType),
            bool => (typeof(bool), BooleanType.Default),
            Dictionary<string, object?> d => (typeof(object), InferStructType(d)),
            List<object?> l => (typeof(object), InferListType(l)),
            _ => (typeof(string), StringType.Default)
        };
    }

    // Inference declares no width: a decimal it finds, nested ones included, carries the mark that says so.
    private static Field InferredField(string name, IArrowType type)
        => type is Decimal128Type
            ? ArrowTypeMap.GetField(name, ArrowTypeMap.DefaultDecimal)
            : new Field(name, type, true);

    private IArrowType InferStructType(Dictionary<string, object?> dict)
    {
        var fields = new List<Field>();
        foreach (var prop in dict)
        {
            var (_, arrowType) = InferTypes(prop.Key, prop.Value);
            fields.Add(InferredField(prop.Key, arrowType));
        }
        return new StructType(fields);
    }

    private IArrowType InferListType(List<object?> list)
    {
        if (list.Any())
        {
            var first = list.First();
            var (_, itemArrowType) = InferTypes("", first);
            return new ListType(InferredField("item", itemArrowType));
        }
        return new ListType(StringType.Default);
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        int batchSize = 1000;
        if (_streamReader is null || Columns is null || Schema is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var batch = new List<Dictionary<string, object?>>(batchSize);
        long number = 0;

        await foreach (var record in EnumerateRawRecordsAsync(ct))
        {
            var dict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = record };
            RefuseKeyOutsideSchema(dict, ++number);
            batch.Add(dict);

            if (batch.Count >= batchSize)
            {
                yield return await SerializeAsync(batch, number - batch.Count + 1);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            yield return await SerializeAsync(batch, number - batch.Count + 1);
        }
    }

    private async Task<RecordBatch> SerializeAsync(List<Dictionary<string, object?>> batch, long firstRecord)
    {
        try
        {
            return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!);
        }
        catch (Apache.Arrow.Serialization.ArrowValueException fault)
        {
            throw TypedTextCell.Unfit(fault, Schema!, "JSONL", firstRecord);
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_streamReader is null || Columns is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var batch = new object?[batchSize][];
        var index = 0;
        long number = 0;

        await foreach (var record in EnumerateRawRecordsAsync(ct))
        {
            var dict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = record };
            RefuseKeyOutsideSchema(dict, ++number);
            batch[index++] = MapDictToRow(dict);

            if (index >= batchSize)
            {
                yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
                batch = new object?[batchSize][];
                index = 0;
            }
        }

        if (index > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
        }
    }

    /// <summary>
    /// A key the schema has no column for would be dropped without a word: stop at the record that carries it.
    /// </summary>
    private void RefuseKeyOutsideSchema(Dictionary<string, object?> record, long number)
    {
        var path = _shape?.FirstUnknownKey(record);
        if (path == null) return;

        throw new InvalidOperationException(_shapeIsDeclared
            ? $"JSONL record {number}: key '{path}' is not in the declared schema. Add it to the schema file given to '--schema-load'."
            : $"JSONL record {number}: key '{path}' is not in the schema inferred from the first {_options.MaxSample} record(s). " +
              "Raise '--max-sample' so the sample reaches it.");
    }

    private object?[] MapDictToRow(Dictionary<string, object?> dict)
    {
        var row = new object?[Columns!.Count];

        for (int i = 0; i < Columns.Count; i++)
        {
            var colName = Columns[i].Name;
            row[i] = GetValueFromFlattenedDict(dict, colName);
        }

        return row;
    }

    private object? GetValueFromFlattenedDict(Dictionary<string, object?> dict, string dotPath)
    {
        if (dotPath.IndexOf('.') == -1) return dict.GetValueOrDefault(dotPath);

        var parts = dotPath.Split('.');
        object? current = dict;
        for (int i = 0; i < parts.Length; i++)
        {
            if (current is Dictionary<string, object?> d)
            {
                if (!d.TryGetValue(parts[i], out current)) return null;
            }
            else return null;
        }
        return current;
    }

    // typedColumns is null outside the read itself: a sample taken to suggest types reads values as
    // the JSON gives them.
    private Dictionary<string, object?> ParseLineToDictionary(string line, Dictionary<string, TypedColumn>? typedColumns = null)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        var dict = new Dictionary<string, object?>();

        foreach (var prop in root.EnumerateObject())
        {
            if (typedColumns != null && typedColumns.TryGetValue(prop.Name, out var typed))
            {
                dict[prop.Name] = ReadTyped(prop.Value, typed);
                continue;
            }
            var isText = _textColumns != null && _textColumns.Contains(prop.Name);
            dict[prop.Name] = ToValue(prop.Value, isText);
        }

        return dict;
    }

    private object? ToRecordValue(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            var dict = new Dictionary<string, object?>();
            foreach (var prop in element.EnumerateObject())
            {
                if (_typedColumns != null && _typedColumns.TryGetValue(prop.Name, out var typed))
                {
                    dict[prop.Name] = ReadTyped(prop.Value, typed);
                    continue;
                }
                var isText = _textColumns != null && _textColumns.Contains(prop.Name);
                dict[prop.Name] = ToValue(prop.Value, isText);
            }
            return dict;
        }
        return ToValue(element, false);
    }

    private object? ReadTyped(JsonElement value, TypedColumn column)
    {
        var read = column.Read(value);
        if (ReferenceEquals(read, TypedTextCell.Unreadable))
            Refuse(column, value);
        return read;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Refuse(TypedColumn column, JsonElement value)
    {
        var where = string.IsNullOrEmpty(_options.Path) ? $"JSONL line {_position}" : $"JSONL record {_position}";
        if (column.Nested != null && column.Nested.FirstUnreadable(value, column.Name) is { } at)
        {
            throw new ColumnValueRefusedException(
                where, at.Path,
                _typesInferred ? $"inferred as {at.Expected} from the first {_options.MaxSample} record(s)" : $"declared {at.Expected} by the schema",
                $"holds a value that does not read as {at.Expected}",
                _typesInferred
                    ? $"Declare column '{column.Name}' with '--column-types' ('string' keeps its JSON text), or raise " +
                      "'--max-sample' so the sample reaches values of the other kind."
                    : $"Correct the record, or declare column '{column.Name}' 'string' in the schema, which keeps its JSON text.");
        }

        if (!_typesInferred)
            TypedTextCell.Refuse(where, column.Name, column.Type, sampledRows: null);

        var hint = DtPipe.Core.Helpers.TypeHelper.HintFor(column.Type) ?? column.Type.Name;
        throw new ColumnValueRefusedException(
            where, column.Name, $"inferred as {hint} from the first {_options.MaxSample} record(s)",
            $"holds a value that does not read as {hint}",
            "Declare the column's type with '--column-types' ('string' keeps the value as text), or raise " +
            "'--max-sample' so the sample reaches values of the other kind.");
    }

    internal static object? ToValue(JsonElement element, bool isTextColumn = false)
    {
        if (isTextColumn)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => element.GetString(),
                _ => element.GetRawText()
            };
        }

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => ToNumber(element),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ToValue(p.Value)),
            JsonValueKind.Array => element.EnumerateArray().Select(e => ToValue(e)).ToList<object?>(),
            JsonValueKind.Null => null,
            _ => element.GetRawText()
        };
    }

    /// <summary>
    /// A JSON number under the narrowest type that keeps its digits, by the rule of the other text readers:
    /// an int64, then a double, then a decimal when it is written as one, and text otherwise.
    /// </summary>
    private static object ToNumber(JsonElement element)
    {
        if (element.TryGetInt64(out var integer)) return integer;

        var text = element.GetRawText();
        if (TextNumber.DoubleHolds(text, out var value)) return value;

        return TextNumber.HasDecimalForm(text, out var exact) ? exact : text;
    }

    // ── Type hint helpers ─────────────────────────────────────────────────────

    private static Dictionary<string, string> ParseColumnTypesOption(string spec)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(spec)) return result;
        foreach (var entry in spec.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var idx = entry.IndexOf(':');
            if (idx <= 0) continue;
            var name = entry[..idx].Trim();
            var typeName = entry[(idx + 1)..].Trim();
            if (!string.IsNullOrEmpty(name) && !string.IsNullOrEmpty(typeName))
                result[name] = typeName;
        }
        return result;
    }

    private bool _disposed;

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancelled before the enumerator is disposed: JsonStreamingPathReader's background producer
        // only unblocks from a channel write by observing this token, never from Dispose alone.
        _pathElementsCts.Cancel();

        if (_pathElementEnumerator != null)
        {
            await _pathElementEnumerator.DisposeAsync();
            _pathElementEnumerator = null;
        }
        _pathElementsCts.Dispose();

        if (_streamReader != null)
        {
            _streamReader.Dispose();
            _streamReader = null;
        }

        if (_fileStream != null)
        {
            await _fileStream.DisposeAsync();
            _fileStream = null;
        }
    }
}
