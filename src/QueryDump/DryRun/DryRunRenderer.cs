namespace QueryDump.DryRun;

using QueryDump.Core;
using Spectre.Console;

/// <summary>
/// Builds Spectre.Console tables for dry-run trace visualization.
/// </summary>
public class DryRunRenderer
{
    /// <summary>
    /// Calculates maximum column widths across all samples for stable layout.
    /// Returns array of widths: [ColumnName, Input, Step1, Step2, ..., Output]
    /// </summary>
    public int[] CalculateMaxWidths(
        List<SampleTrace> samples,
        List<string> stepNames)
    {
        if (samples.Count == 0) return Array.Empty<int>();
        
        // Column count: ColumnName + Input + Steps + Output
        int columnCount = 2 + stepNames.Count + 1;
        var maxWidths = new int[columnCount];
        
        // Initialize with header widths
        maxWidths[0] = "Column".Length;
        maxWidths[1] = "Input".Length;
        for (int s = 0; s < stepNames.Count; s++)
        {
            maxWidths[2 + s] = Math.Max(maxWidths[2 + s], stepNames[s].Length + 10); // "(Step N)"
        }
        maxWidths[columnCount - 1] = "Output".Length;
        
        // Scan all samples for max content widths
        foreach (var trace in samples)
        {
            var finalSchema = trace.Schemas.Last();
            
            foreach (var col in finalSchema)
            {
                // Column name width
                var colNameLen = col.Name.Length + (col.IsVirtual ? 10 : 0); // "(virtual)"
                maxWidths[0] = Math.Max(maxWidths[0], colNameLen);
                
                // Values for input + each step
                for (int step = 0; step < trace.Schemas.Count; step++)
                {
                    var schema = trace.Schemas[step];
                    var values = trace.Values.Count > step ? trace.Values[step] : null;
                    
                    var idx = -1;
                    for (int k = 0; k < schema.Count; k++)
                    {
                        if (schema[k].Name == col.Name) { idx = k; break; }
                    }
                    
                    if (idx >= 0 && values != null && idx < values.Length)
                    {
                        var v = values[idx];
                        var rawVal = v?.ToString() ?? "null";
                        var typeName = v?.GetType().Name ?? "null";
                        // Approximate display length: value + " (Type)"
                        var displayLen = rawVal.Length + typeName.Length + 3;
                        maxWidths[1 + step] = Math.Max(maxWidths[1 + step], displayLen);
                    }
                }
                
                // Output column (same as last step but in separate column)
                var finalVals = trace.Values.Last();
                var finalIdx = -1;
                for (int k = 0; k < finalSchema.Count; k++)
                {
                    if (finalSchema[k].Name == col.Name) { finalIdx = k; break; }
                }
                if (finalIdx >= 0 && finalIdx < finalVals.Length)
                {
                    var v = finalVals[finalIdx];
                    var rawVal = v?.ToString() ?? "null";
                    var typeName = v?.GetType().Name ?? "null";
                    var displayLen = rawVal.Length + typeName.Length + 3;
                    maxWidths[columnCount - 1] = Math.Max(maxWidths[columnCount - 1], displayLen);
                }
            }
        }
        
        // Add padding and cap at reasonable max
        for (int i = 0; i < maxWidths.Length; i++)
        {
            maxWidths[i] = Math.Min(maxWidths[i] + 2, 50); // Cap at 50 chars
        }
        
        return maxWidths;
    }

    /// <summary>
    /// Builds a trace table for a single sample showing values at each pipeline stage.
    /// </summary>
    public Table BuildTraceTable(
        int sampleIndex,
        int totalSamples,
        SampleTrace trace,
        List<string> stepNames,
        int[]? columnWidths = null)
    {
        var table = new Table().Border(TableBorder.Rounded);
        
        // Header with sample index
        var header = totalSamples > 1 
            ? $"[green]Pipeline Trace Analysis[/] — Record {sampleIndex + 1}/{totalSamples}"
            : "[green]Pipeline Trace Analysis[/]";
        table.Title = new TableTitle(header);
        
        // Columns: Column Name | Input | Step1 | Step2 | ... | Output
        int colIdx = 0;
        
        var colNameColumn = new TableColumn("Column");
        if (columnWidths != null && colIdx < columnWidths.Length)
            colNameColumn.Width = columnWidths[colIdx];
        table.AddColumn(colNameColumn);
        colIdx++;
        
        var inputColumn = new TableColumn("Input");
        if (columnWidths != null && colIdx < columnWidths.Length)
            inputColumn.Width = columnWidths[colIdx];
        table.AddColumn(inputColumn);
        colIdx++;
        
        for (int s = 0; s < stepNames.Count; s++)
        {
            var stepColumn = new TableColumn($"[yellow]{stepNames[s]} (Step {s + 1})[/]");
            if (columnWidths != null && colIdx < columnWidths.Length)
                stepColumn.Width = columnWidths[colIdx];
            table.AddColumn(stepColumn);
            colIdx++;
        }
        
        var outputColumn = new TableColumn("[green]Output[/]");
        if (columnWidths != null && colIdx < columnWidths.Length)
            outputColumn.Width = columnWidths[colIdx];
        table.AddColumn(outputColumn);
        
        // Final schema is the last in trace
        var finalSchema = trace.Schemas.Last();
        
        foreach (var col in finalSchema)
        {
            var rowMarkup = new List<string>();
            
            // Column Name
            rowMarkup.Add(col.IsVirtual ? $"{col.Name} [grey](virtual)[/]" : col.Name);
            
            string lastValue = "";
            
            // Values for input + each step
            for (int step = 0; step < trace.Schemas.Count; step++)
            {
                var schema = trace.Schemas[step];
                var values = trace.Values.Count > step ? trace.Values[step] : null;
                
                // Find column index by name in this step's schema
                var idx = -1;
                for (int k = 0; k < schema.Count; k++)
                {
                    if (schema[k].Name == col.Name)
                    {
                        idx = k;
                        break;
                    }
                }

                string displayVal;
                string rawVal;
                
                if (idx == -1 || values == null || idx >= values.Length)
                {
                    displayVal = "";
                    rawVal = "N/A_NOT_EXIST";
                }
                else
                {
                    var v = values[idx];
                    rawVal = v?.ToString() ?? "";
                    var typeName = v?.GetType().Name ?? "null";
                    var typeSuffix = $" [dim]({typeName})[/]";
                    displayVal = v is null ? "[grey]null[/]" : Markup.Escape(rawVal) + typeSuffix;
                }
                
                // Highlight Logic: Green if new, Yellow if modified
                if (step > 0 && rawVal != "N/A_NOT_EXIST")
                {
                    bool isNew = (idx != -1) && (lastValue == "N/A_NOT_EXIST");
                    bool isMod = (idx != -1) && (!isNew) && (rawVal != lastValue);
                    
                    if (isNew) displayVal = $"[green]{displayVal}[/]";
                    else if (isMod) displayVal = $"[yellow]{displayVal}[/]";
                    
                    if (idx != -1) lastValue = rawVal;
                    else lastValue = "N/A_NOT_EXIST";
                }
                else
                {
                    if (idx != -1) lastValue = rawVal;
                    else lastValue = "N/A_NOT_EXIST";
                }

                rowMarkup.Add(displayVal);
            }

            // Final Output Column
            {
                var finalVals = trace.Values.Last();
                var finalIdx = -1;
                for (int k = 0; k < finalSchema.Count; k++)
                {
                    if (finalSchema[k].Name == col.Name)
                    {
                        finalIdx = k;
                        break;
                    }
                }
                
                if (finalIdx >= 0 && finalIdx < finalVals.Length)
                {
                    var v = finalVals[finalIdx];
                    var typeName = v?.GetType().Name ?? "null";
                    var typeSuffix = $" [dim]({typeName})[/]";
                    var valStr = v is null ? "[grey]null[/]" : Markup.Escape(v.ToString() ?? "") + typeSuffix;
                    rowMarkup.Add($"[blue]{valStr}[/]");
                }
                else
                {
                    rowMarkup.Add("");
                }
            }
            
            table.AddRow(rowMarkup.ToArray());
        }

        return table;
    }
}

