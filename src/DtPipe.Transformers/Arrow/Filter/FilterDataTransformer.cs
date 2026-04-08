using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Filter;

public partial class FilterDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Filter.FilterOptions>
{
	private readonly DtPipe.Transformers.Arrow.Filter.FilterOptions _options;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly List<JsValue> _compiledFilters = new();

	[GeneratedRegex(@"^(\w+)\s*(==|!=|>|<|>=|<=)\s*(.+)$", RegexOptions.Compiled)]
	private static partial Regex SimpleFilterPattern();

	public override bool CanProcessColumnar { get; protected set; }

	public FilterDataTransformer(DtPipe.Transformers.Arrow.Filter.FilterOptions options, IJsEngineProvider jsEngineProvider)
	{
		_options = options;
		_jsEngineProvider = jsEngineProvider;
	}

	private string[]? _columnNames;

	// Cache wrapped scripts for fast re-compilation
	private readonly List<string> _wrappedScripts = new();

	public override ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
	{
		if (_options.Filters == null || _options.Filters.Length == 0)
		{
			CanProcessColumnar = true;
			return ValueTask.FromResult(sourceColumns);
		}

		_columnNames = sourceColumns.Select(c => c.Name).ToArray();
		var engine = _jsEngineProvider.GetEngine();

		bool allSimple = true;
		var simpleFilters = new List<SimpleFilterInfo>();

		// Compile filters
		for (int i = 0; i < _options.Filters.Length; i++)
		{
			var filterScript = _options.Filters[i].Trim();

			// Try simple detection
			var match = SimpleFilterPattern().Match(filterScript);
			if (match.Success)
			{
				var colName = match.Groups[1].Value;
				var op = match.Groups[2].Value;
				var valStr = match.Groups[3].Value.Trim();

				var colIdx = System.Array.FindIndex(_columnNames, c => c.Equals(colName, StringComparison.OrdinalIgnoreCase));
				if (colIdx >= 0)
				{
					simpleFilters.Add(new SimpleFilterInfo(colIdx, op, valStr));
				}
				else
				{
					allSimple = false;
				}
			}
			else
			{
				allSimple = false;
			}

			var funcName = $"__filter_{Guid.NewGuid():N}";

			string body = filterScript;
			if (!body.Contains("return ") && !body.EndsWith(";"))
			{
				body = "return " + body + ";";
			}

			// Wrap in function expression
			var wrappedScript = $"function(row) {{ {body} }}";
			_wrappedScripts.Add(wrappedScript);

			// Compile in initial engine (validation)
			engine.SetValue(funcName, engine.Evaluate($"({wrappedScript})"));
			_compiledFilters.Add(new JsString(funcName));
		}

		CanProcessColumnar = allSimple;
		if (allSimple)
		{
			_simpleFilters = simpleFilters;
		}

		return ValueTask.FromResult(sourceColumns);
	}

	private List<SimpleFilterInfo>? _simpleFilters;

	private record SimpleFilterInfo(int ColumnIndex, string Operator, string RawValue);

	protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (!CanProcessColumnar || _compiledFilters.Count == 0) return new ValueTask<RecordBatch?>(batch);

		// Vectorized filtering logic
		var selectionMask = new bool[batch.Length];
		for (int i = 0; i < batch.Length; i++) selectionMask[i] = true;

		foreach (var filter in _simpleFilters!)
		{
			var column = batch.Column(filter.ColumnIndex);
			for (int i = 0; i < batch.Length; i++)
			{
				if (!selectionMask[i]) continue;
				var val = ArrowTypeMapper.GetValueForField(column, batch.Schema.GetFieldByIndex(filter.ColumnIndex), i);
				selectionMask[i] = EvaluateSimple(val, filter.Operator, filter.RawValue);
			}
		}

		// Count selected
		int selectedCount = 0;
		for (int i = 0; i < batch.Length; i++) if (selectionMask[i]) selectedCount++;

		if (selectedCount == 0) return new ValueTask<RecordBatch?>(result: null);
		if (selectedCount == batch.Length) return new ValueTask<RecordBatch?>(batch);

		// Build new batch by picking values (Compact)
		var newArrays = new List<IArrowArray>();
		for (int colIdx = 0; colIdx < batch.Schema.FieldsList.Count; colIdx++)
		{
			var original = batch.Column(colIdx);
			newArrays.Add(CompactArray(original, selectionMask, selectedCount));
		}

		return new ValueTask<RecordBatch?>(new RecordBatch(batch.Schema, newArrays, selectedCount));
	}

	private bool EvaluateSimple(object? val, string op, string rawVal)
	{
		var valStr = val?.ToString();
		var targetVal = rawVal.Trim('\'', '\"');

		// Handle null
		if (rawVal.Equals("null", StringComparison.OrdinalIgnoreCase))
		{
			return op switch {
				"==" => val == null,
				"!=" => val != null,
				_ => false
			};
		}

		if (val == null) return op == "!=";

		// Value comparison
		return op switch {
			"==" => valStr == targetVal,
			"!=" => valStr != targetVal,
			">" => Compare(val, targetVal) > 0,
			"<" => Compare(val, targetVal) < 0,
			">=" => Compare(val, targetVal) >= 0,
			"<=" => Compare(val, targetVal) <= 0,
			_ => false
		};
	}

	private int Compare(object val, string target)
	{
		if (val is double d1 && double.TryParse(target, out var d2)) return d1.CompareTo(d2);
		if (val is int i1 && int.TryParse(target, out var i2)) return i1.CompareTo(i2);
		if (val is long l1 && long.TryParse(target, out var l2)) return l1.CompareTo(l2);
		return string.Compare(val.ToString(), target, StringComparison.Ordinal);
	}

	private IArrowArray CompactArray(IArrowArray original, bool[] mask, int count)
	{
		// Fallback: build manually via builder
		var builder = ArrowTypeMapper.CreateBuilder(original.Data.DataType);
		for (int i = 0; i < mask.Length; i++)
		{
			if (mask[i])
			{
				ArrowTypeMapper.AppendValue(builder, ArrowTypeMapper.GetValue(original, i));
			}
		}
		return ArrowTypeMapper.BuildArray(builder);
	}


	public override object?[]? Transform(IReadOnlyList<object?> row)
	{
		if (_compiledFilters.Count == 0 || _columnNames == null) return row as object?[] ?? row.ToArray();

		var engine = _jsEngineProvider.GetEngine();
		EnsureFiltersCompiled(engine);

		// Build JS Context with Proxy for missing column detection
		var jsSource = new JsObject(engine);
		for (int i = 0; i < row.Count; i++)
		{
			var val = row[i];
			if (val == DBNull.Value) val = null;
			jsSource.Set(_columnNames[i], JsValue.FromObject(engine, val));
		}

        // Create Proxy using Jint Engine API for robust schema validation
        engine.SetValue("__source", jsSource);
        var jsRow = engine.Evaluate("new Proxy(__source, { get: (target, prop) => { if (typeof prop === 'string' && !(prop in target)) throw new ReferenceError(`Column '${prop}' not found in schema`); return target[prop]; } })");

		// Set 'row' in global scope for Evaluate Call
		engine.SetValue("row", jsRow);

		foreach (var funcName in _compiledFilters)
		{
			try
			{
				var result = engine.Evaluate($"{funcName}(row)");
				if (!result.IsBoolean() || !result.AsBoolean())
				{
					return null; // Drop row if result is false or not boolean
				}
			}
			catch (Exception ex)
			{
                // If it's a ReferenceError (column missing), rethrow to fail the process if strict.
                // Otherwise, treat property access on null as 'false' (permissive mode for existing columns).
                if (ex.Message.Contains("ReferenceError")) throw;
				return null;
			}
		}


		return row as object?[] ?? row.ToArray();
	}

	private void EnsureFiltersCompiled(Engine engine)
	{
		if (_compiledFilters.Count > 0)
		{
			var firstFunc = _compiledFilters[0].ToString();
			var val = engine.GetValue(firstFunc);

			if (val.IsUndefined() || val.IsNull())
			{
				for (int i = 0; i < _compiledFilters.Count; i++)
				{
					// Evaluate function expression
					var script = _wrappedScripts[i]; // "function(row) { ... }"
													 // Wrap in parens to ensure expression evaluation
					var funcVal = engine.Evaluate($"({script})");
					engine.SetValue(_compiledFilters[i].ToString(), funcVal);
				}
			}
		}
	}
}
