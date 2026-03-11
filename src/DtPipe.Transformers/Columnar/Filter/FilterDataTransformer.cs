using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Columnar.Filter;

public partial class FilterDataTransformer : IColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Columnar.Filter.FilterTransformerOptions>
{
	private readonly DtPipe.Transformers.Columnar.Filter.FilterTransformerOptions _options;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly List<JsValue> _compiledFilters = new();

	[GeneratedRegex(@"^(\w+)\s*(==|!=|>|<|>=|<=)\s*(.+)$", RegexOptions.Compiled)]
	private static partial Regex SimpleFilterPattern();

	public bool CanProcessColumnar { get; private set; }

	public FilterDataTransformer(DtPipe.Transformers.Columnar.Filter.FilterTransformerOptions options, IJsEngineProvider jsEngineProvider)
	{
		_options = options;
		_jsEngineProvider = jsEngineProvider;
	}

	private string[]? _columnNames;

	// Cache wrapped scripts for fast re-compilation
	private readonly List<string> _wrappedScripts = new();

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
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

	public ValueTask<RecordBatch?> TransformBatchAsync(RecordBatch batch, CancellationToken ct = default)
	{
		if (!CanProcessColumnar || _compiledFilters.Count == 0) return new ValueTask<RecordBatch?>(batch);

		// Vectorized filtering logic
		// To keep it simple for now, we'll evaluate rows in a loop but avoiding Jint overhead
		// A truly vectorized implementation would use Arrow.Compute or specialized buffer loops.

		var selectionMask = new bool[batch.Length];
		for (int i = 0; i < batch.Length; i++) selectionMask[i] = true;

		foreach (var filter in _simpleFilters!)
		{
			var column = batch.Column(filter.ColumnIndex);
			for (int i = 0; i < batch.Length; i++)
			{
				if (!selectionMask[i]) continue;
				var val = GetValueFromArrow(column, i);
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
		var builder = CreateBuilder(original.Data.DataType);
		for (int i = 0; i < mask.Length; i++)
		{
			if (mask[i])
			{
				AppendValueToBuilder(builder, GetValueFromArrow(original, i));
			}
		}
		return BuildArray(builder);
	}

	private static object? GetValueFromArrow(IArrowArray array, int index)
	{
		if (array.IsNull(index)) return null;

		return array switch {
			StringArray a => a.GetString(index),
			Int32Array a => a.GetValue(index),
			Int64Array a => a.GetValue(index),
			DoubleArray a => a.GetValue(index),
			FloatArray a => a.GetValue(index),
			BooleanArray a => a.GetValue(index),
			Date64Array a => a.GetDateTime(index),
			TimestampArray a => a.GetTimestamp(index),
			_ => null
		};
	}

	private static IArrowArrayBuilder CreateBuilder(IArrowType type)
	{
		return type.TypeId switch
		{
			ArrowTypeId.Boolean => new BooleanArray.Builder(),
			ArrowTypeId.Int32 => new Int32Array.Builder(),
			ArrowTypeId.Int64 => new Int64Array.Builder(),
			ArrowTypeId.Double => new DoubleArray.Builder(),
			ArrowTypeId.Float => new FloatArray.Builder(),
			ArrowTypeId.String => new StringArray.Builder(),
			ArrowTypeId.Timestamp => new TimestampArray.Builder(),
			ArrowTypeId.Date64 => new Date64Array.Builder(),
			_ => new StringArray.Builder()
		};
	}

	private static void AppendValueToBuilder(IArrowArrayBuilder builder, object? value)
	{
		if (value == null) { AppendNull(builder); return; }

		if (builder is StringArray.Builder s) s.Append(value.ToString() ?? "");
		else if (builder is Int32Array.Builder i32) i32.Append(Convert.ToInt32(value));
		else if (builder is Int64Array.Builder i64) i64.Append(Convert.ToInt64(value));
		else if (builder is DoubleArray.Builder dbl) dbl.Append(Convert.ToDouble(value));
		else if (builder is FloatArray.Builder flt) flt.Append(Convert.ToSingle(value));
		else if (builder is BooleanArray.Builder b && value is bool bv) b.Append(bv);
		else if (builder is Date64Array.Builder d64 && value is DateTime dt) d64.Append(dt);
		else if (builder is TimestampArray.Builder ts && value is DateTimeOffset dto) ts.Append(dto);
		else AppendNull(builder);
	}

	private static void AppendNull(IArrowArrayBuilder builder)
	{
		switch (builder)
		{
			case BooleanArray.Builder b: b.AppendNull(); break;
			case Int32Array.Builder b: b.AppendNull(); break;
			case Int64Array.Builder b: b.AppendNull(); break;
			case DoubleArray.Builder b: b.AppendNull(); break;
			case FloatArray.Builder b: b.AppendNull(); break;
			case StringArray.Builder b: b.AppendNull(); break;
			case TimestampArray.Builder b: b.AppendNull(); break;
			case Date64Array.Builder b: b.AppendNull(); break;
			default: if (builder is StringArray.Builder s) s.AppendNull(); break;
		}
	}

	private static IArrowArray BuildArray(IArrowArrayBuilder builder)
	{
		return builder switch
		{
			BooleanArray.Builder b => b.Build(),
			Int32Array.Builder b => b.Build(),
			Int64Array.Builder b => b.Build(),
			DoubleArray.Builder b => b.Build(),
			FloatArray.Builder b => b.Build(),
			StringArray.Builder b => b.Build(),
			TimestampArray.Builder b => b.Build(),
			Date64Array.Builder b => b.Build(),
			_ => throw new NotSupportedException($"Unsupported builder type for BuildArray: {builder.GetType().Name}")
		};
	}

	public object?[]? Transform(object?[] row)
	{
		if (_compiledFilters.Count == 0 || _columnNames == null) return row;

		var engine = _jsEngineProvider.GetEngine();
		EnsureFiltersCompiled(engine);

		// Build JS Context with Proxy for missing column detection
		var jsSource = new JsObject(engine);
		for (int i = 0; i < row.Length; i++)
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


		return row;
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
