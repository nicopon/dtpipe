namespace QueryDump.DryRun;

using QueryDump.Core;
using Spectre.Console;

/// <summary>
/// Orchestrates dry-run execution: collects samples, processes through pipeline, and renders.
/// </summary>
public class DryRunService
{
    private const int SoftLimit = 100;

    /// <summary>
    /// Runs dry-run with the specified sample count.
    /// </summary>
    public async Task RunAsync(
        IStreamReader reader,
        List<IDataTransformer> pipeline,
        int sampleCount,
        IAnsiConsole console,
        CancellationToken ct)
    {
        // Warning for large sample counts
        if (sampleCount > SoftLimit)
        {
            console.MarkupLine($"[yellow]⚠ Warning: Collecting {sampleCount} samples (>{SoftLimit}) may use significant memory.[/]");
        }

        console.WriteLine();
        console.MarkupLine($"[grey]Fetching {sampleCount} sample row(s) for trace analysis...[/]");

        // 1. Capture schema evolution through pipeline
        var traceSchemas = new List<IReadOnlyList<ColumnInfo>>();
        var stepNames = new List<string>();

        var simSchema = reader.Columns!;
        traceSchemas.Add(simSchema); // Input step

        foreach (var t in pipeline)
        {
            simSchema = await t.InitializeAsync(simSchema, ct);
            traceSchemas.Add(simSchema);
            stepNames.Add(t.GetType().Name.Replace("DataTransformer", ""));
        }

        // 2. Collect samples and process each through pipeline
        var samples = new List<SampleTrace>();
        int collected = 0;

        await foreach (var batch in reader.ReadBatchesAsync(Math.Min(sampleCount, 1000), ct))
        {
            for (int i = 0; i < batch.Length && collected < sampleCount; i++)
            {
                var row = batch.Span[i].ToArray();
                var trace = ProcessRowThroughPipeline(row, pipeline, traceSchemas);
                samples.Add(trace);
                collected++;
            }

            if (collected >= sampleCount) break;
        }

        if (samples.Count == 0)
        {
            console.MarkupLine("[red]No rows returned by query.[/]");
            return;
        }

        console.MarkupLine($"[grey]Collected {samples.Count} sample(s).[/]");

        // 3. Display
        var renderer = new DryRunRenderer();
        
        // Calculate fixed column widths for stable layout across all samples
        var columnWidths = renderer.CalculateMaxWidths(samples, stepNames);

        // Interactive navigation (works for 1 or more samples)
        console.WriteLine();
        if (samples.Count > 1)
        {
            console.MarkupLine("[dim]Launching interactive viewer...[/]");
        }
        
        var navigator = new DryRunNavigator(renderer, console);
        navigator.Navigate(samples, stepNames, columnWidths);
    }

    /// <summary>
    /// Processes a single row through all pipeline transformers, capturing values at each stage.
    /// </summary>
    private SampleTrace ProcessRowThroughPipeline(
        object?[] inputRow,
        List<IDataTransformer> pipeline,
        List<IReadOnlyList<ColumnInfo>> traceSchemas)
    {
        var values = new List<object?[]>();
        
        // Clone input to protect history
        values.Add((object?[])inputRow.Clone());

        var currentRow = inputRow;
        foreach (var t in pipeline)
        {
            currentRow = t.Transform(currentRow);
            values.Add((object?[])currentRow.Clone());
        }

        return new SampleTrace(traceSchemas, values);
    }
}
