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
        var errorIndices = FindErrorIndices(result.Samples, targetInfo);
        int initialIndex = errorIndices.Count > 0 ? errorIndices[0] : 0;

        if (errorIndices.Count > 0)
        {
            _console.MarkupLine($"[yellow]Auto-focusing on first record with errors (Record {initialIndex + 1}/{result.Samples.Count}) - Found {errorIndices.Count} problematic records.[/]");
        }

        var navigator = new DryRunNavigator(renderer, _console);
        navigator.Navigate(result.Samples, result.StepNames, columnWidths, result.SchemaInspectionError, targetInfo, initialIndex, errorIndices);
    }
    
    private List<int> FindErrorIndices(List<SampleTrace> samples, TargetSchemaInfo? targetInfo)
    {
        var indices = new List<int>();
        if (targetInfo == null || !targetInfo.Exists) return indices;

        for (int i = 0; i < samples.Count; i++)
        {
            var finalStage = samples[i].Stages.Last();
            var schema = finalStage.Schema;
            var values = finalStage.Values;
            
            bool hasViolation = false;

            for (int k = 0; k < schema.Count; k++)
            {
                if (k >= values.Length) break;
                
                var colName = schema[k].Name;
                var targetCol = targetInfo.Columns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                
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
