using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Helpers;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Row.Expand;

public class ExpandDataTransformer : IMultiRowTransformer, IRequiresOptions<DtPipe.Transformers.Row.Expand.ExpandOptions>
{
	private readonly DtPipe.Transformers.Row.Expand.ExpandOptions _options;
	private readonly IJsEngineProvider _jsEngineProvider;
	private readonly List<JsValue> _compiledExpands = new();
	private readonly List<string> _wrappedScripts = new();
	private readonly List<string> _sourceExpressions = new();
	private string[]? _columnNames;
	private readonly HashSet<string> _reportedUnknownKeys = new(StringComparer.OrdinalIgnoreCase);

	public ExpandDataTransformer(DtPipe.Transformers.Row.Expand.ExpandOptions options, IJsEngineProvider jsEngineProvider)
	{
		_options = options;
		_jsEngineProvider = jsEngineProvider;
	}

	public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
	{
		if (_options.Expand == null || _options.Expand.Length == 0)
		{
			return ValueTask.FromResult(sourceColumns);
		}

		_columnNames = sourceColumns.Select(c => c.Name).ToArray();
		var engine = _jsEngineProvider.GetEngine();

		for (int i = 0; i < _options.Expand.Length; i++)
		{
			var expandScript = _options.Expand[i];
			var funcName = $"__expand_{Guid.NewGuid():N}";

			string body = expandScript.Trim();
			if (!body.Contains("return ") && !body.EndsWith(";"))
			{
				body = "return " + body + ";";
			}

			// Wrap in function expression
			var wrappedScript = $"function(row) {{ {body} }}";

			// Register globally
			engine.SetValue(funcName, engine.Evaluate($"({wrappedScript})"));

			_wrappedScripts.Add(wrappedScript);
			_sourceExpressions.Add(expandScript.Trim());

			_compiledExpands.Add(new JsString(funcName));
		}

		var updatedColumns = new List<PipeColumnInfo>(sourceColumns);
		foreach (KeyValuePair<string, string> entry in _options.ExpandTypes)
		{
			string col = entry.Key;
			Type type = ParseDeclaredType(col, entry.Value);

			var idx = updatedColumns.FindIndex(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase));
			if (idx >= 0)
				updatedColumns[idx] = updatedColumns[idx] with { ClrType = type };
			else
				updatedColumns.Add(new PipeColumnInfo(col, type, IsNullable: true));
		}

		// Appended, never inserted: TransformMany indexes _columnNames by the incoming row's
		// position, so a declared column has to sit past the last source column.
		_columnNames = updatedColumns.Select(c => c.Name).ToArray();

		return ValueTask.FromResult<IReadOnlyList<PipeColumnInfo>>(updatedColumns);
	}

	// Required by IDataTransformer (base interface)
	public object?[]? Transform(IReadOnlyList<object?> row)
	{
		var results = TransformMany(row);
		return results.FirstOrDefault();
	}

	public IEnumerable<object?[]> TransformMany(IReadOnlyList<object?> row)
	{
		if (_compiledExpands.Count == 0 || _columnNames == null)
		{
			yield return row as object?[] ?? row.ToArray();
			yield break;
		}

		var engine = _jsEngineProvider.GetEngine();
		EnsureFunctionsCompiled(engine);

		// Build JS Context with Proxy for missing column detection
		var jsSource = new JsObject(engine);
		for (int i = 0; i < row.Count; i++)
		{
			var val = row[i];
			if (val == DBNull.Value) val = null;
			jsSource.Set(_columnNames[i], JsValue.FromObject(engine, val));
		}

        // Wrap in Proxy
        engine.SetValue("__source", jsSource);
        var jsRow = engine.Evaluate("new Proxy(__source, { get: (target, prop) => { if (typeof prop === 'string' && !(prop in target)) throw new ReferenceError(`Column '${prop}' not found in schema`); return target[prop]; } })");

		// Helper to process a list of rows through a specific expand function
		IEnumerable<object?[]> currentRows = new[] { row as object?[] ?? row.ToArray() };

		for (int e = 0; e < _compiledExpands.Count; e++)
		{
			var funcName = _compiledExpands[e];
			var expression = _sourceExpressions[e];
			var nextRows = new List<object?[]>();

			foreach (var r in currentRows)
			{
				JsValue currentJsRow;
				if (_compiledExpands.Count == 1)
				{
					currentJsRow = jsRow; // Use the one built above
				}
				else
				{
					// Rebuild for intermediate rows
					var intermediateSource = new JsObject(engine);
					for (int k = 0; k < r.Length; k++)
					{
						var val = r[k];
						if (val == DBNull.Value) val = null;
						intermediateSource.Set(_columnNames[k], JsValue.FromObject(engine, val));
					}
                    engine.SetValue("__intermediate", intermediateSource);
					currentJsRow = engine.Evaluate("new Proxy(__intermediate, { get: (target, prop) => { if (typeof prop === 'string' && !(prop in target)) throw new ReferenceError(`Column '${prop}' not found in schema`); return target[prop]; } })");
				}

				// Set 'row' in global scope for Evaluate Call
				engine.SetValue("row", currentJsRow);

				JsValue result;
				try
				{
					result = engine.Evaluate($"{funcName}(row)");
				}
				catch (Exception ex)
				{
					// The generated symbol and the Jint wrapper used to go to the user, who had no
					// way to connect '__expand_80f14686' to anything they had written.
					throw new InvalidOperationException(
						$"--expand could not evaluate {Quote(expression)}: {ex.Message}. "
					  + "It takes a JavaScript expression over 'row' — a bare column name is not one, "
					  + "write 'row.<column>'.", ex);
				}

				// A result of the wrong shape used to yield no rows at all, with no message and
				// exit code 0 — a whole source silently discarded.
				if (!TryGetElements(engine, result, out var elements))
					throw new InvalidOperationException(
						$"--expand expects {Quote(expression)} to return an array of row objects, "
					  + $"but it returned {Describe(result)}.");

				foreach (var item in elements)
				{
					if (!item.IsObject())
						throw new InvalidOperationException(
							$"--expand expects {Quote(expression)} to return an array of row objects, "
						  + $"but an element of the array is {Describe(item)}. Map each element onto a row — "
						  + "'row.tags.map(t => ({ ...row, tag: t }))' — and declare the column it adds with "
						  + "--expand-types \"tag:string\".");

					var obj = item.AsObject();
					ReportKeysWithNoColumn(obj, expression);

					var newRow = new object?[_columnNames.Length];
					for (int c = 0; c < _columnNames.Length; c++)
					{
						var val = obj.Get(_columnNames[c]);
						newRow[c] = val.IsUndefined() || val.IsNull() ? null : val.ToObject();
					}
					nextRows.Add(newRow);
				}
			}
			currentRows = nextRows;
		}

		foreach (var r in currentRows)
		{
			yield return r;
		}
	}

	/// <summary>
	/// A key the output schema does not carry is dropped: the schema is fixed before the first
	/// row, so there is nowhere to put it. Say it once per key rather than per row — the run
	/// stays valid, but nothing disappears without a word.
	/// </summary>
	private void ReportKeysWithNoColumn(Jint.Native.Object.ObjectInstance obj, string expression)
	{
		foreach (var property in obj.GetOwnProperties())
		{
			var key = property.Key.ToString();
			if (Array.Exists(_columnNames!, c => c.Equals(key, StringComparison.OrdinalIgnoreCase))) continue;
			if (!_reportedUnknownKeys.Add(key)) continue;

			Console.Error.WriteLine(
				$"[dtpipe] Warning: --expand {Quote(expression)} sets '{key}', which is not a column of the "
			  + $"output, so it is dropped. Declare it with --expand-types \"{key}:string\" to keep it.");
		}
	}

	/// <summary>
	/// The elements of a result, when it has any.
	/// </summary>
	/// <remarks>
	/// A nested JSON array reaches the engine as a wrapped CLR collection: it carries
	/// <c>length</c> and answers <c>.map</c>, but <c>Array.isArray</c> is false and so is
	/// <see cref="JsValue.IsArray"/>. Judging the result on that alone reported <c>row.tags</c> as
	/// "not an array", which is not what a reader sees in the file.
	/// </remarks>
	private static bool TryGetElements(Engine engine, JsValue value, out IEnumerable<JsValue> elements)
	{
		if (value.IsArray())
		{
			elements = value.AsArray();
			return true;
		}

		if (value.IsObject() && value.ToObject() is System.Collections.IEnumerable clr and not string)
		{
			elements = clr.Cast<object?>().Select(o => JsValue.FromObject(engine, o)).ToList();
			return true;
		}

		elements = Array.Empty<JsValue>();
		return false;
	}

	/// <summary>Names a JavaScript value by shape, for a message about a result of the wrong one.</summary>
	private static string Describe(JsValue value) => value.Type switch
	{
		Jint.Runtime.Types.String => "a string",
		Jint.Runtime.Types.Number => "a number",
		Jint.Runtime.Types.Boolean => "a boolean",
		Jint.Runtime.Types.Undefined => "undefined",
		Jint.Runtime.Types.Null => "null",
		Jint.Runtime.Types.Object => "a single object",
		_ => "a value of another kind"
	};

	/// <summary>The user's expression, trimmed to stay readable inside a one-line message.</summary>
	private static string Quote(string expression)
		=> expression.Length <= 60 ? $"'{expression}'" : $"'{expression[..57]}...'";

	/// <summary>The hint a declaration carries, defaulting to string when it names only a column.</summary>
	private static Type ParseDeclaredType(string column, string? hint)
		=> string.IsNullOrWhiteSpace(hint)
			? typeof(string)
			: TypeHelper.RequireTypeHint("--expand-types", column, hint);

	private void EnsureFunctionsCompiled(Engine engine)
	{
		if (_compiledExpands.Count > 0)
		{
			var firstFunc = _compiledExpands[0].ToString();
			var val = engine.GetValue(firstFunc);
			if (val.IsUndefined())
			{
				for (int i = 0; i < _compiledExpands.Count; i++)
				{
					var script = _wrappedScripts[i];
					var name = _compiledExpands[i].ToString();
					engine.SetValue(name, engine.Evaluate($"({script})"));
				}
			}
		}
	}
}
