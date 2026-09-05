using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli;
using DtPipe.Core.Validation;
using DtPipe.DryRun;
using Spectre.Console;

namespace DtPipe.Cli.DryRun;

/// <summary>
/// Renders a sample run's report: execution plan, target compatibility, key and constraint
/// findings, then the navigable per-row trace.
/// </summary>
public class DryRunCliController
{
	private readonly IAnsiConsole _console;

	public DryRunCliController(IAnsiConsole console)
	{
		_console = console;
	}

	/// <summary>
	/// Renders what a sample run observed. It does not produce the analysis: the run already
	/// happened, on the real execution path, and this is handed its result. The controller
	/// having once owned an analyser of its own is what let a second engine live here.
	/// </summary>
	public async Task RenderAsync(
		SampleReport result,
		PipelineExecutionPlan? executionPlan = null,
		bool isInteractive = true,
		CancellationToken ct = default)
	{
		await Task.CompletedTask;

		if (!isInteractive) return;

		if (result.Samples.Count == 0)
		{
			_console.MarkupLine("[red]No rows returned by query.[/]");
			return;
		}

		var renderer = new DryRunRenderer();
		var stageTotals = result.Run.Stages.Select(s => s.TotalSeen).ToList();

		// 3. Render Execution Plan
		if (executionPlan != null)
		{
			renderer.RenderExecutionPlan(executionPlan, _console);
		}

		// 3.5. Render Compatibility Report if exists
		if (result.CompatibilityReport != null)
		{
			renderer.RenderCompatibilityReport(result.CompatibilityReport, _console);

			if (result.CompatibilityReport.Warnings.Count > 0 || result.CompatibilityReport.Errors.Count > 0)
			{
				_console.WriteLine();
                WaitIfInteractive("to continue to trace analysis...");
			}
		}
		else if (result.SchemaInspectionError != null)
		{
			_console.MarkupLine($"[yellow]⚠ Could not inspect target schema: {Markup.Escape(result.SchemaInspectionError)}[/]");
			_console.WriteLine();
            WaitIfInteractive("to continue to trace analysis...");
		}

		// 3.5. Render Primary Key Validation
		if (result.KeyValidation != null)
		{
			renderer.RenderKeyValidation(result.KeyValidation, _console);

			// If there's an error, pause before continuing
			if (!result.KeyValidation.IsValid && result.KeyValidation.IsRequired)
			{
				WaitIfInteractive("to continue to trace analysis...");
			}
		}

		// 3.6. Render Data Constraint Validation
		if (result.ConstraintValidation != null)
		{
			renderer.RenderConstraintValidation(result.ConstraintValidation, _console);

			// If there's an error, pause before continuing
			if (result.ConstraintValidation.Errors != null && result.ConstraintValidation.Errors.Count > 0)
			{
				WaitIfInteractive("to continue to trace analysis...");
			}
		}

		// 3.7. Render Performance Hints
		if (result.PerformanceHints != null && result.PerformanceHints.Count > 0)
		{
			renderer.RenderPerformanceHints(result.PerformanceHints, _console);
		}

		// 4. Calculate Layout
		var hasSchemaWarning = !string.IsNullOrEmpty(result.SchemaInspectionError);
		var targetInfo = result.CompatibilityReport?.TargetInfo;
		var columnWidths = renderer.CalculateMaxWidths(result.Samples, result.StepNames, hasSchemaWarning, targetInfo);

		// 5. Interactive Navigation
		_console.WriteLine();
		if (result.Samples.Count > 1)
		{
			_console.MarkupLine("[dim]Launching interactive viewer...[/]");
		}

		// Find errors for navigation
		var errorIndices = FindErrorIndices(result.Samples, targetInfo, result.Dialect);
		int initialIndex = errorIndices.Count > 0 ? errorIndices[0] : 0;

		if (errorIndices.Count > 0)
		{
			_console.MarkupLine($"[yellow]Auto-focusing on first record with errors (Record {initialIndex + 1}/{result.Samples.Count}) - Found {errorIndices.Count} problematic records.[/]");
		}

        if (CanBlockOnKeyboard())
        {
            var navigator = new DryRunNavigator(renderer, _console);
            navigator.Navigate(result.Samples, result.StepNames, columnWidths, result.SchemaInspectionError, targetInfo, initialIndex, errorIndices, stageTotals);
        }
        else
        {
            _console.MarkupLine("[grey]Non-interactive mode: rendering first sample trace only.[/]");
            _console.Write(renderer.BuildTraceTable(0, result.Samples.Count, result.Samples[0], result.StepNames, columnWidths, result.SchemaInspectionError, targetInfo, stageTotals));
            _console.WriteLine();
            RenderSafetyFooter(result);
        }
	}

    /// <summary>
    /// Says what the run can support, and no more. "No data written" is true of the writer;
    /// whether the SOURCE could have been modified is a different question, and answering the
    /// second with the first is how a reassuring message becomes a false one.
    /// </summary>
    private void RenderSafetyFooter(SampleReport result)
    {
        _console.MarkupLine("[green]Sample run complete. The writer was neutralised — no data was written to the target.[/]");
        _console.MarkupLine(result.Enforcement == DtPipe.Sessions.ReadOnlyEnforcement.ServerEnforced
            ? "[grey]Source protection: the database session was read-only — the server itself refused writes.[/]"
            : "[grey]Source protection: a conservative verb scan only. This engine has no read-only session, so source-side effects are not proven absent.[/]");
    }

    private void WaitIfInteractive(string message)
    {
        if (CanBlockOnKeyboard())
        {
            _console.MarkupLine($"[dim]Press any key {message}[/]");
            Console.ReadKey(true);
        }
    }

    /// <summary>Whether it is safe to block this call waiting for a keypress: a real terminal is
    /// attached AND no caller has declared itself an unattended, LLM-driven invocation
    /// (<see cref="NonInteractiveGuard"/>) — the two checks answer different questions and neither
    /// substitutes for the other (see the guard's own doc comment).</summary>
    private bool CanBlockOnKeyboard() =>
        !NonInteractiveGuard.IsSuppressed
        && _console.Profile.Capabilities.Interactive
        && !Console.IsInputRedirected
        && !Console.IsOutputRedirected;


	private List<int> FindErrorIndices(List<SampleTrace> samples, TargetSchemaInfo? targetInfo, ISqlDialect? dialect)
	{
		var indices = new List<int>();
		if (targetInfo == null || !targetInfo.Exists) return indices;

		// Pre-compute lookup for performance
		// But samples all share the same schema (final stage).

		if (samples.Count == 0) return indices;
		var schema = samples[0].Stages.Last().Schema;

		// Build map: SourceIndex -> TargetColumnInfo (or null)
		var columnMap = new TargetColumnInfo?[schema.Count];
		var remainingTargetCols = targetInfo.Columns.ToList();

		// Replicate matching logic from SchemaCompatibilityAnalyzer for consistency.
		// SchemaCompatibilityAnalyzer consumes target columns as it matches.
		for (int k = 0; k < schema.Count; k++)
		{
			var srcCol = schema[k];
			TargetColumnInfo? tgtCol = null;

			if (dialect != null)
			{
				string effectivePhysicalName;
				if (srcCol.IsCaseSensitive || dialect.NeedsQuoting(srcCol.Name))
				{
					effectivePhysicalName = srcCol.Name;
				}
				else
				{
					effectivePhysicalName = dialect.Normalize(srcCol.Name);
				}
				tgtCol = remainingTargetCols.FirstOrDefault(c => c.Name.Equals(effectivePhysicalName, StringComparison.Ordinal));
			}
			else
			{
				tgtCol = remainingTargetCols.FirstOrDefault(c => c.Name.Equals(srcCol.Name, StringComparison.OrdinalIgnoreCase));
			}

			if (tgtCol != null)
			{
				columnMap[k] = tgtCol;
				remainingTargetCols.Remove(tgtCol);
			}
		}

		for (int i = 0; i < samples.Count; i++)
		{
			var finalStage = samples[i].Stages.Last();
			var values = finalStage.Values;

			bool hasViolation = false;

			for (int k = 0; k < schema.Count; k++)
			{
				if (values == null || k >= values.Length) break;

				var targetCol = columnMap[k];

				if (targetCol != null)
				{
					var result = SchemaValidator.Validate(values[k], targetCol);
					if (result.HasAnyViolation)
					{
						hasViolation = true;
						break;
					}
				}
			}

			if (hasViolation) indices.Add(i);
		}
		return indices;
	}
}
