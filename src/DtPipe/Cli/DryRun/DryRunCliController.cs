namespace DtPipe.Cli.DryRun;

using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DtPipe.DryRun;
using Spectre.Console;

/// <summary>
/// CLI Controller for Dry Run execution.
/// Orchestrates the analysis and visualization logic.
/// </summary>
public class DryRunCliController
{
	private const int SoftLimit = 100;
	private readonly IAnsiConsole _console;

	public DryRunCliController(IAnsiConsole console)
	{
		_console = console;
	}

	/// <summary>
	/// Executes the dry run workflow: Analyzes data and displays interactive results.
	/// </summary>
	public async Task RunAsync(
		IStreamReader reader,
		List<IDataTransformer> pipeline,
		int sampleCount,
		IDataWriter? writer = null,
		CancellationToken ct = default)
	{
		// 1. User Feedback for Analysis
		if (sampleCount > SoftLimit)
		{
			_console.MarkupLine($"[yellow]⚠ Warning: Collecting {sampleCount} samples (>{SoftLimit}) may use significant memory.[/]");
		}

		_console.WriteLine();
		_console.MarkupLine($"[grey]Fetching {sampleCount} sample row(s) for trace analysis...[/]");

		// 2. Run Core Analysis
		var analyzer = new DryRunAnalyzer();
		ISchemaInspector? inspector = writer as ISchemaInspector;

		// Notify user about inspection
		if (inspector != null)
		{
			_console.WriteLine();
			_console.MarkupLine("[grey]Inspecting target schema...[/]");
		}

		DryRunResult result;
		try
		{
			result = await analyzer.AnalyzeAsync(reader, pipeline, sampleCount, inspector, ct);
		}
		catch (Exception ex)
		{
			_console.MarkupLine($"[red]Analysis failed: {Markup.Escape(ex.Message)}[/]");
			return;
		}

		if (result.Samples.Count == 0)
		{
			_console.MarkupLine("[red]No rows returned by query.[/]");
			return;
		}

		_console.MarkupLine($"[grey]Collected {result.Samples.Count} sample(s).[/]");

		// 3. Render Compatibility Report if exists
		var renderer = new DryRunRenderer();

		if (result.CompatibilityReport != null)
		{
			renderer.RenderCompatibilityReport(result.CompatibilityReport, _console);

			if (result.CompatibilityReport.Warnings.Count > 0 || result.CompatibilityReport.Errors.Count > 0)
			{
				_console.WriteLine();
				_console.MarkupLine("[dim]Press any key to continue to trace analysis...[/]");
				Console.ReadKey(true);
			}
		}
		else if (result.SchemaInspectionError != null)
		{
			_console.MarkupLine($"[yellow]⚠ Could not inspect target schema: {Markup.Escape(result.SchemaInspectionError)}[/]");
			_console.WriteLine();
			_console.MarkupLine("[dim]Press any key to continue to trace analysis...[/]");
			Console.ReadKey(true);
		}

		// 3.5. Render Primary Key Validation
		if (result.KeyValidation != null)
		{
			renderer.RenderKeyValidation(result.KeyValidation, _console);

			// If there's an error, pause before continuing
			if (!result.KeyValidation.IsValid && result.KeyValidation.IsRequired)
			{
				_console.MarkupLine("[dim]Press any key to continue to trace analysis...[/]");
				Console.ReadKey(true);
			}
		}

		// 3.6. Render Data Constraint Validation
		if (result.ConstraintValidation != null)
		{
			renderer.RenderConstraintValidation(result.ConstraintValidation, _console);

			// If there's an error, pause before continuing
			if (result.ConstraintValidation.Errors != null && result.ConstraintValidation.Errors.Count > 0)
			{
				_console.MarkupLine("[dim]Press any key to continue to trace analysis...[/]");
				Console.ReadKey(true);
			}
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

		var navigator = new DryRunNavigator(renderer, _console);
		navigator.Navigate(result.Samples, result.StepNames, columnWidths, result.SchemaInspectionError, targetInfo, initialIndex, errorIndices);
	}

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

		// We must replicate the matching logic from SchemaCompatibilityAnalyzer to be consistent
		// Note: SchemaCompatibilityAnalyzer consumes target columns as it matches. 
		// We should do the same to ensure 1:1 mapping if possible.
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
