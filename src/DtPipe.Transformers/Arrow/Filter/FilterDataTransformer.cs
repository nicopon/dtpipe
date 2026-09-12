using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using System.Globalization;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Filter;

public partial class FilterDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Filter.FilterOptions>
{
	private readonly DtPipe.Transformers.Arrow.Filter.FilterOptions _options;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly List<JsValue> _compiledFilters = new();

	// Matches the DOCUMENTED spelling only. Keying the fast path on a bare column name served a
	// different semantics to whoever found it, and the two-character operators must precede their
	// prefixes or ">=" parses as ">" against the value "= 500" — which matched no row at all.
	[GeneratedRegex(@"^row\.(\w+)\s*(==|!=|>=|<=|>|<)\s*(.+)$", RegexOptions.Compiled)]
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

	public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
	{
		await base.InitializeAsync(sourceColumns, cancellationToken);
		if (_options.Filters == null || _options.Filters.Length == 0)
		{
			CanProcessColumnar = true;
			return sourceColumns;
		}

		_columnNames = sourceColumns.Select(c => c.Name).ToArray();
		var engine = _jsEngineProvider.GetEngine();

		bool allSimple = true;
		var simpleFilters = new List<SimpleFilterInfo>();

		// Compile filters
		for (int i = 0; i < _options.Filters.Length; i++)
		{
			var filterScript = _options.Filters[i].Trim();

			var match = SimpleFilterPattern().Match(filterScript);
			if (match.Success && TryBuildSimpleFilter(match, sourceColumns, out var simple))
				simpleFilters.Add(simple);
			else
				allSimple = false;

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

		return sourceColumns;
	}

	private List<SimpleFilterInfo>? _simpleFilters;

	private enum LiteralKind { Number, Text }

	private record SimpleFilterInfo(int ColumnIndex, string Operator, LiteralKind Kind, double Number, string Text);

	/// <summary>
	/// Whether the vectorised path can answer this expression with the same result Jint would give.
	/// It is an optimisation, so it may decline — never disagree.
	/// </summary>
	/// <remarks>
	/// Two shapes qualify by construction: a numeric column against an unquoted numeric literal
	/// (JavaScript compares two numbers) and a text column against a quoted literal under == or !=
	/// (it compares two strings). Everything else — a quoted literal on a relational operator, a
	/// null or boolean literal, a temporal or binary column — goes to Jint, rather than to a second
	/// hand-written copy of JavaScript's coercion rules that would drift from the first.
	/// </remarks>
	private static bool TryBuildSimpleFilter(Match match, IReadOnlyList<PipeColumnInfo> columns, out SimpleFilterInfo filter)
	{
		filter = null!;
		var colName = match.Groups[1].Value;
		var op = match.Groups[2].Value;
		var raw = match.Groups[3].Value.Trim();

		var colIdx = -1;
		for (int i = 0; i < columns.Count; i++)
		{
			if (columns[i].Name.Equals(colName, StringComparison.OrdinalIgnoreCase)) { colIdx = i; break; }
		}
		if (colIdx < 0) return false;

		var clrType = Nullable.GetUnderlyingType(columns[colIdx].ClrType) ?? columns[colIdx].ClrType;

		if (TryReadStringLiteral(raw, out var text))
		{
			if (clrType != typeof(string) || (op != "==" && op != "!=")) return false;
			filter = new SimpleFilterInfo(colIdx, op, LiteralKind.Text, 0, text);
			return true;
		}

		if (IsNumeric(clrType) && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var number))
		{
			filter = new SimpleFilterInfo(colIdx, op, LiteralKind.Number, number, string.Empty);
			return true;
		}

		return false;
	}

	/// <summary>A single quoted literal, and not an expression that merely starts and ends with a quote.</summary>
	private static bool TryReadStringLiteral(string raw, out string text)
	{
		text = string.Empty;
		if (raw.Length < 2) return false;
		var quote = raw[0];
		if (quote != '\'' && quote != '"') return false;
		if (raw[^1] != quote) return false;
		if (raw.IndexOf(quote, 1) != raw.Length - 1) return false;
		text = raw[1..^1];
		return true;
	}

	private static bool IsNumeric(Type t) =>
		t == typeof(sbyte) || t == typeof(byte) || t == typeof(short) || t == typeof(ushort)
		|| t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong)
		|| t == typeof(float) || t == typeof(double) || t == typeof(decimal);

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
				selectionMask[i] = EvaluateSimple(val, filter);
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

	/// <summary>
	/// The comparison JavaScript would make, for the two shapes <see cref="TryBuildSimpleFilter"/>
	/// admits. A null reads as 0 under a relational operator, the way ToNumber(null) does, but
	/// never equals a number — loose equality matches only null and undefined.
	/// </summary>
	private static bool EvaluateSimple(object? val, SimpleFilterInfo filter)
	{
		if (val is DBNull) val = null;

		if (filter.Kind == LiteralKind.Text)
		{
			var text = val as string;
			return filter.Operator == "==" ? text == filter.Text : text != filter.Text;
		}

		var number = val is null ? 0d : Convert.ToDouble(val, CultureInfo.InvariantCulture);
		return filter.Operator switch
		{
			"==" => val is not null && number == filter.Number,
			"!=" => val is null || number != filter.Number,
			">" => number > filter.Number,
			"<" => number < filter.Number,
			">=" => number >= filter.Number,
			"<=" => number <= filter.Number,
			_ => false
		};
	}

	private IArrowArray CompactArray(IArrowArray original, bool[] mask, int count)
	{
		// Fallback: build manually via builder
		var builder = ArrowTypeMapper.CreateBuilder(original.Data.DataType);
		var append = ArrowTypeMapper.ResolveAppender(builder);
		for (int i = 0; i < mask.Length; i++)
		{
			if (mask[i])
			{
				append(ArrowTypeMapper.GetValue(original, i));
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

		for (int i = 0; i < _compiledFilters.Count; i++)
		{
			try
			{
				var result = engine.Evaluate($"{_compiledFilters[i]}(row)");
				if (!result.IsBoolean() || !result.AsBoolean())
				{
					return null; // Drop row if result is false or not boolean
				}
			}
			catch (JavaScriptException jsEx) when (IsMissingColumn(jsEx))
			{
				var expression = _options.Filters?[i] ?? "(unknown)";
				throw new InvalidOperationException(
					$"Error evaluating filter '{expression}': {jsEx.Message}. "
					+ "A filter reads its columns off 'row' — write row.<column>.", jsEx);
			}
			catch (Exception)
			{
				// Property access on a null value: the filter reads it as "no match", on purpose.
				return null;
			}
		}


		return row as object?[] ?? row.ToArray();
	}

	/// <summary>
	/// A column absent from the schema and a property read on a null value are different failures,
	/// and the filter must not answer both with "no match".
	/// </summary>
	/// <remarks>
	/// Matched on the JS error's own <c>name</c>. The Proxy raises a ReferenceError whose MESSAGE is
	/// "Column 'X' not found in schema", so a test for the word "ReferenceError" inside that message
	/// could never fire: every row was dropped instead, and a filter naming a column that did not
	/// exist wrote a zero-byte file and exited 0.
	/// </remarks>
	private static bool IsMissingColumn(JavaScriptException ex) =>
		ex.Error is ObjectInstance error && error.Get("name").ToString() == "ReferenceError";

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
