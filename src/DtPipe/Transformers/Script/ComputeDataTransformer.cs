using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Services;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Script;

/// <summary>
/// Transforms data rows using Javascript scripts via Jint.
/// </summary>
public sealed class ComputeDataTransformer : IDataTransformer, IRequiresOptions<ComputeOptions>
{
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly List<string> _initializationScripts = new();
	private readonly Dictionary<string, string> _mappings;
	private readonly Dictionary<string, Type> _typeHints;

	// State initialized in InitializeAsync
	private Dictionary<string, int>? _columnNameToIndex;
	private string[]? _columnNames;
	private ScriptColumnProcessor[]? _processors;
	private int _inputColumnCount; // number of columns in the original input schema (before new virtual columns)

	private readonly bool _skipNull;

	public ComputeDataTransformer(ComputeOptions options, IJsEngineProvider jsEngineProvider)
	{
		_jsEngineProvider = jsEngineProvider;
		_mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		_typeHints = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

		foreach (var (col, typeStr) in options.ComputeTypes)
		{
			var t = DtPipe.Core.Helpers.TypeHelper.ParseTypeHint(typeStr);
			if (t != null) _typeHints[col] = t;
		}

		foreach (var mapping in options.Compute)
		{
			var parts = mapping.Split(':', 3);
			if (parts.Length == 3 && DtPipe.Core.Helpers.TypeHelper.ParseTypeHint(parts[1]) is { } t)
			{
				_typeHints[parts[0]] = t;
				_mappings[parts[0]] = parts[2];
			}
			else if (mapping.Split(':', 2) is { Length: 2 } p2)
			{
				_mappings[p2[0]] = p2[1];
			}
		}

		_skipNull = options.SkipNull;
	}

	public bool HasScript => _mappings.Count > 0;

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (!HasScript)
		{
			return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
		}

		_inputColumnCount = columns.Count;

		// Build mutable name→index mapping (we may extend it for new virtual columns)
		var nameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
		for (var i = 0; i < columns.Count; i++)
			nameToIndex[columns[i].Name] = i;

		var outputColumns = new List<PipeColumnInfo>(columns);
		var processors = new List<ScriptColumnProcessor>();
		var engine = _jsEngineProvider.GetEngine();

		foreach (var (colName, script) in _mappings)
		{
			// If column doesn't exist yet, create it as a new virtual column
			if (!nameToIndex.TryGetValue(colName, out var colIndex))
			{
				colIndex = outputColumns.Count;
				nameToIndex[colName] = colIndex;
				// Default to string; may be overridden by _typeHints below
				outputColumns.Add(new PipeColumnInfo(colName, typeof(string), IsNullable: true));
			}

			string body = script.Trim();
			if (!body.Contains("return ") && !body.EndsWith(";"))
				body = "return " + body + ";";

			var uniqueFunctionName = $"proc_{colIndex}_{Guid.NewGuid():N}";
			var wrappedScript = $"function(row) {{ {body} }}";
			engine.SetValue(uniqueFunctionName, engine.Evaluate($"({wrappedScript})"));

			processors.Add(new ScriptColumnProcessor(colIndex, uniqueFunctionName, wrappedScript));
		}

		// Apply type hints to existing or new columns
		foreach (var (col, type) in _typeHints)
		{
			if (nameToIndex.TryGetValue(col, out var idx))
				outputColumns[idx] = outputColumns[idx] with { ClrType = type };
		}

		_columnNameToIndex = nameToIndex;
		_columnNames = outputColumns.Select(c => c.Name).ToArray();
		_processors = processors.ToArray();

		return new ValueTask<IReadOnlyList<PipeColumnInfo>>(outputColumns);
	}

	public object?[]? Transform(object?[] row)
	{
		if (_processors == null || _processors.Length == 0 || _columnNames == null) return row;

		var engine = _jsEngineProvider.GetEngine();
		EnsureFunctionsCompiled(engine);

		// Expand the row if new virtual columns were added during InitializeAsync
		var outputLength = _columnNames.Length;
		if (outputLength > row.Length)
		{
			var expanded = new object?[outputLength];
			Array.Copy(row, expanded, row.Length);
			row = expanded;
		}

		// Build a JS object representing the input row (input columns only)
		var jsRow = new JsObject(engine);
		for (int i = 0; i < _inputColumnCount && i < _columnNames.Length; i++)
		{
			jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
		}

		engine.SetValue("row", jsRow);

		foreach (var processor in _processors)
		{
			if (_skipNull && processor.ColumnIndex < _inputColumnCount && row[processor.ColumnIndex] is null)
				continue;

			try
			{
				var result = engine.Evaluate($"{processor.FunctionName}(row)");

				row[processor.ColumnIndex] = (result.IsUndefined() || result.IsNull())
					? null
					: result.ToObject();
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException($"Error evaluating compute script for column '{_columnNames[processor.ColumnIndex]}': {ex.Message}", ex);
			}
		}

		return row;
	}

	// Dispose handled by DI scope? No, provider is singleton/scoped.
	// ComputeDataTransformer does not own the engine anymore.

	private void EnsureFunctionsCompiled(Engine engine)
	{
		if (_processors != null && _processors.Length > 0)
		{
			var firstFunc = _processors[0].FunctionName;
			var val = engine.GetValue(firstFunc);
			if (val.IsUndefined())
			{
				foreach (var p in _processors)
				{
					// Re-register using SetValue/Evaluate
					engine.SetValue(p.FunctionName, engine.Evaluate($"({p.WrappedScript})"));
				}
			}
		}
	}

	private record struct ScriptColumnProcessor(int ColumnIndex, string FunctionName, string WrappedScript);
}
