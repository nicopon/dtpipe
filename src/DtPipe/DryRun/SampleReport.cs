using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.DryRun;

/// <summary>
/// Everything a sample run has to show: what it observed, and what that implies about the
/// target. Assembled after the run, from the run — there is no second pass over the data.
/// </summary>
public sealed record SampleReport(
	SampleRun Run,
	List<SampleTrace> Samples,
	List<string> StepNames,
	SchemaCompatibilityReport? CompatibilityReport,
	string? SchemaInspectionError,
	ISqlDialect? Dialect = null,
	KeyValidationResult? KeyValidation = null,
	ConstraintValidationResult? ConstraintValidation = null,
	IReadOnlyDictionary<string, string>? PerformanceHints = null,
	/// <summary>
	/// What the run could actually guarantee about not writing. "No data written" is a claim
	/// about the writer; the source is a separate question, and a report must not answer one
	/// with the other. A verb scan does not prove a query is read-only — SELECT my_function()
	/// passes it — so the promise stops where the proof does.
	/// </summary>
	DtPipe.Sessions.ReadOnlyEnforcement Enforcement = DtPipe.Sessions.ReadOnlyEnforcement.VerbScanOnly,
	/// <summary>Which branch this report is about. A DAG produces one per branch.</summary>
	string? BranchAlias = null,
	/// <summary>The content-addressed checkpoint this run materialised, when --checkpoint was set.</summary>
	string? CheckpointKey = null);

public static class SampleRunExtensions
{
	/// <summary>A column one stage created and a later stage wrote over.</summary>
	/// <param name="Column">The column name.</param>
	/// <param name="ProducedBy">Stage that first put it in the schema.</param>
	/// <param name="ReplacedBy">Stage whose output no longer carries the produced values.</param>
	public sealed record Replacement(string Column, string ProducedBy, string ReplacedBy);

	/// <summary>
	/// Columns a transformer produced and a later transformer wrote over, read off the sampled
	/// values rather than off the scripts that made them.
	///
	/// <para>
	/// It reports, it does not judge: overwriting produced data is sometimes the point — a faked
	/// name feeding a computed address is the same shape as a faked date replaced by today's, and
	/// only the author knows which one was meant. A recorded session shipped both in one branch
	/// without noticing either.
	/// </para>
	///
	/// <para>
	/// A column the <em>reader</em> supplied is never reported: overwriting a source value is what
	/// anonymisation is. And stages of differing row counts are not compared, because after an
	/// expand or a window there is no row <c>j</c> running through both.
	/// </para>
	/// </summary>
	public static List<Replacement> ProducedThenReplaced(this SampleRun run)
	{
		var found = new List<Replacement>();
		if (run.Stages.Count < 3) return found;   // reader + at least two transformers

		var producedAt = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
		for (var i = 0; i < run.Stages.Count; i++)
			foreach (var col in run.Stages[i].Schema)
				if (!producedAt.ContainsKey(col.Name)) producedAt[col.Name] = i;

		for (var i = 1; i < run.Stages.Count; i++)
		{
			var before = run.Stages[i - 1];
			var after = run.Stages[i];
			if (before.Rows.Count != after.Rows.Count || after.Rows.Count == 0) continue;

			foreach (var col in after.Schema)
			{
				if (!producedAt.TryGetValue(col.Name, out var origin) || origin == 0 || origin >= i) continue;

				var b = IndexOf(before.Schema, col.Name);
				var a = IndexOf(after.Schema, col.Name);
				if (b < 0 || a < 0) continue;

				if (Differs(before.Rows, b, after.Rows, a))
					found.Add(new Replacement(col.Name, run.Stages[origin].Name, after.Name));
			}
		}

		return found;
	}

	private static int IndexOf(IReadOnlyList<PipeColumnInfo> schema, string name)
	{
		for (var i = 0; i < schema.Count; i++)
			if (string.Equals(schema[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
		return -1;
	}

	private static bool Differs(IReadOnlyList<object?[]> before, int b, IReadOnlyList<object?[]> after, int a)
	{
		for (var j = 0; j < before.Count; j++)
		{
			if (b >= before[j].Length || a >= after[j].Length) continue;
			if (!string.Equals(before[j][b]?.ToString(), after[j][a]?.ToString(), StringComparison.Ordinal))
				return true;
		}
		return false;
	}

	/// <summary>
	/// Presents a <see cref="SampleRun"/> as the row-major traces the renderer navigates: trace
	/// <c>j</c> is row <c>j</c> of every stage, side by side.
	///
	/// The correspondence is exact only while the pipeline is 1:1. A stage that expands or
	/// aggregates has a different row count from its neighbour, and there is then no single
	/// "row j" running through the whole chain — a shorter stage simply has no cell in that
	/// column. That is a fact about the pipeline, not a defect of the view; the renderer reads
	/// each stage's <see cref="StageCapture.TotalSeen"/> to say where the cardinality changed
	/// rather than implying a correspondence that does not exist.
	/// </summary>
	public static List<SampleTrace> ToTraces(this SampleRun run)
	{
		var traces = new List<SampleTrace>();
		if (run.Stages.Count == 0) return traces;

		var depth = run.Stages.Max(s => s.Rows.Count);
		for (var j = 0; j < depth; j++)
		{
			var stages = new List<StageTrace>(run.Stages.Count);
			foreach (var stage in run.Stages)
				stages.Add(new StageTrace(stage.Schema, j < stage.Rows.Count ? stage.Rows[j] : null));

			traces.Add(new SampleTrace(stages));
		}

		return traces;
	}

	/// <summary>The rows leaving the last stage — what the pipeline would have written.</summary>
	public static IReadOnlyList<object?[]> FinalRows(this SampleRun run)
		=> run.Stages.Count == 0 ? Array.Empty<object?[]>() : run.Stages[^1].Rows;

	/// <summary>The schema leaving the last stage.</summary>
	public static IReadOnlyList<PipeColumnInfo> FinalSchema(this SampleRun run)
		=> run.Stages.Count == 0 ? Array.Empty<PipeColumnInfo>() : run.Stages[^1].Schema;
}
