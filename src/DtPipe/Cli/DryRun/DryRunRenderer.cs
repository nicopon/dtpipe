namespace DtPipe.Cli.DryRun;

using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DtPipe.DryRun; // For SampleTrace, StageTrace
using Spectre.Console;

/// <summary>
/// Builds Spectre.Console tables for dry-run trace visualization.
/// </summary>
public class DryRunRenderer
{
    #region Style Constants
    
    // Colors for trace table
    private const string ColorOutput = "blue";           // Output column (header + values)
    private const string ColorNewValue = "green";        // New value introduced in pipeline
    private const string ColorModifiedValue = "yellow";  // Modified value in pipeline
    private const string ColorStep = "yellow";           // Pipeline step headers
    private const string ColorNull = "grey";             // Null values
    private const string ColorDim = "dim";               // Secondary/muted text
    private const string ColorWarning = "yellow";        // Warning indicators
    
    // Output column header text
    private const string OutputHeaderNormal = "Output";
    private const string OutputWarningEmoji = "⚠️";
    private const string OutputWarningSuffix = OutputWarningEmoji + " unverified";
    private const string OutputHeaderWithWarningText = OutputHeaderNormal + " (" + OutputWarningSuffix + ")";

    #endregion

    /// <summary>
    /// Calculates maximum column widths across all samples for stable layout.
    /// Returns array of widths: [ColumnName, Input, Step1, Step2, ..., Output]
    /// </summary>
    /// <param name="samples">Sample traces</param>
    /// <param name="stepNames">Pipeline step names</param>
    /// <param name="hasSchemaWarning">If set, Output header will be longer to include warning</param>
    /// <param name="targetSchema">Target schema info (for estimating Output width)</param>
    public int[] CalculateMaxWidths(
        List<SampleTrace> samples,
        List<string> stepNames,
        bool hasSchemaWarning = false,
        TargetSchemaInfo? targetSchema = null)
    {
        if (samples.Count == 0) return Array.Empty<int>();
        
        // Column count: ColumnName + Input + Steps + Output
        // Input is Stage 0. Steps are Stages 1 to N.
        // Stage 0 -> Input
        // Stage 1 -> Step 1
        // ...
        // Last Stage -> Output (this is a duplicate display column for clarity)
        
        int columnCount = 2 + stepNames.Count + 1;
        var maxWidths = new int[columnCount];
        
        // Initialize with header widths
        maxWidths[0] = "Column".Length;
        maxWidths[1] = "Input".Length;
        for (int s = 0; s < stepNames.Count; s++)
        {
            maxWidths[2 + s] = Math.Max(maxWidths[2 + s], stepNames[s].Length + 10); // "(Step N)"
        }
        // Output header length depends on warning state
        var outputHeaderText = hasSchemaWarning ? OutputHeaderWithWarningText : OutputHeaderNormal;
        maxWidths[columnCount - 1] = outputHeaderText.Length;
        
        // Scan all samples for max content widths
        foreach (var trace in samples)
        {
            var finalStage = trace.Stages.Last();
            var finalSchema = finalStage.Schema;
            
            foreach (var col in finalSchema)
            {
                // Column name width
                var colNameLen = col.Name.Length;
                maxWidths[0] = Math.Max(maxWidths[0], colNameLen);
                
                // Values for input + each step
                for (int stageIdx = 0; stageIdx < trace.Stages.Count; stageIdx++)
                {
                    var stage = trace.Stages[stageIdx];
                    var schema = stage.Schema;
                    var values = stage.Values;
                    
                    var idx = -1;
                    for (int k = 0; k < schema.Count; k++)
                    {
                        if (schema[k].Name == col.Name) { idx = k; break; }
                    }
                    
                    // Input is column 1. Step 1 is column 2 (stageIdx 1).
                    // So stageIdx maps directly to maxWidths[1 + stageIdx].

                    // maxWidths[1] = Input.
                    
                    if (idx >= 0 && idx < values.Length)
                    {
                        var v = values[idx];
                        var rawVal = v?.ToString() ?? "null";
                        var typeName = v?.GetType().Name ?? "null";
                        // Approximate display length: value + " (Type)"
                        var displayLen = rawVal.Length + typeName.Length + 3;
                        maxWidths[1 + stageIdx] = Math.Max(maxWidths[1 + stageIdx], displayLen);
                    }
                }
                
                // Output column (same as last step but in separate column)
                // Maps to maxWidths[columnCount - 1]
                var finalVals = finalStage.Values;
                var finalIdx = -1;
                for (int k = 0; k < finalSchema.Count; k++)
                {
                    if (finalSchema[k].Name == col.Name) { finalIdx = k; break; }
                }
                
                if (finalIdx >= 0 && finalIdx < finalVals.Length)
                {
                    var typeName = finalSchema[finalIdx].ClrType.Name;
                    
                    // Default length estimate for simple type display
                    var displayLen = typeName.Length + 5; 

                    // If we have target schema, estimating length is harder but usually longer
                    if (targetSchema != null)
                    {
                         // "{ClrType} -> {NativeType} [PK,UQ]"
                         // Approx 50 chars?
                         displayLen += 30; 
                    }

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
    /// <param name="sampleIndex">Current sample index (0-based)</param>
    /// <param name="totalSamples">Total number of samples</param>
    /// <param name="trace">Sample trace data</param>
    /// <param name="stepNames">Pipeline step names</param>
    /// <param name="columnWidths">Fixed column widths for stable layout</param>
    /// <param name="schemaWarning">Warning message if schema inspection failed (changes Output column header)</param>
    /// <param name="targetSchema">Target schema info (for validation)</param>
    public Table BuildTraceTable(
        int sampleIndex,
        int totalSamples,
        SampleTrace trace,
        List<string> stepNames,
        int[]? columnWidths = null,
        string? schemaWarning = null,
        TargetSchemaInfo? targetSchema = null)
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
            var stepColumn = new TableColumn($"[{ColorStep}]{stepNames[s]} (Step {s + 1})[/]");
            if (columnWidths != null && colIdx < columnWidths.Length)
                stepColumn.Width = columnWidths[colIdx];
            table.AddColumn(stepColumn);
            colIdx++;
        }
        
        // Output column with optional warning indicator (Variante C)
        var outputHeader = string.IsNullOrEmpty(schemaWarning)
            ? $"[{ColorOutput}]{OutputHeaderNormal}[/]"
            : $"[{ColorOutput}]{OutputHeaderNormal}[/] [{ColorDim}]({OutputWarningSuffix})[/]";
        var outputColumn = new TableColumn(outputHeader);
        if (columnWidths != null && colIdx < columnWidths.Length)
            outputColumn.Width = columnWidths[colIdx];
        table.AddColumn(outputColumn);
        
        // Final schema is the last in trace
        var finalStage = trace.Stages.Last();
        var finalSchema = finalStage.Schema;
        
        foreach (var col in finalSchema)
        {
            var rowMarkup = new List<string>();
            
            // Column Name
            rowMarkup.Add(Markup.Escape(col.Name));
            
            string lastValue = "";
            
            // Values for input + each step
            for (int stageIdx = 0; stageIdx < trace.Stages.Count; stageIdx++)
            {
                var stage = trace.Stages[stageIdx];
                var schema = stage.Schema;
                var values = stage.Values;
                
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
                
                if (idx == -1 || idx >= values.Length)
                {
                    displayVal = "";
                    rawVal = "N/A_NOT_EXIST";
                }
                else
                {
                    var v = values[idx];
                    rawVal = v?.ToString() ?? "";
                    var typeName = v?.GetType().Name ?? "null";
                    var typeSuffix = $" [{ColorDim}]({Markup.Escape(typeName)})[/]";
                    displayVal = v is null ? $"[{ColorNull}]null[/]" : Markup.Escape(rawVal) + typeSuffix;
                }
                
                // Highlight Logic: Green if new, Yellow if modified
                // Stage 0 is Input. Stage > 0 is transformed.
                if (stageIdx > 0 && rawVal != "N/A_NOT_EXIST")
                {
                    bool isNew = (idx != -1) && (lastValue == "N/A_NOT_EXIST");
                    bool isMod = (idx != -1) && (!isNew) && (rawVal != lastValue);
                    
                    if (isNew) displayVal = $"[{ColorNewValue}]{displayVal}[/]";
                    else if (isMod) displayVal = $"[{ColorModifiedValue}]{displayVal}[/]";
                    
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
                var finalVals = finalStage.Values;
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
                    var colName = finalSchema[finalIdx].Name;
                    var typeName = finalSchema[finalIdx].ClrType.Name;
                    
                    var sb = new StringBuilder();
                    sb.Append($"[dim]{Markup.Escape(typeName)}[/]");

                    if (targetSchema != null)
                    {
                        var targetCol = targetSchema.Columns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                        if (targetCol != null)
                        {
                            // 1. Validation via Core logic
                            var validation = SchemaValidator.Validate(v, targetCol);

                            // 2. Build Display String
                            sb.Append(" -> ");
                            
                            // Native Type
                            var nativeTypeMarkup = $"[blue]{Markup.Escape(targetCol.NativeType)}[/]";
                            if (validation.HasAnyViolation)
                            {
                                nativeTypeMarkup = $"[red]{Markup.Escape(targetCol.NativeType)}[/]";
                            }
                            sb.Append(nativeTypeMarkup);
                            
                            // Constraints
                            var constraints = new List<string>();
                            if (targetCol.IsPrimaryKey) constraints.Add("PK");
                            if (targetCol.IsUnique) constraints.Add("UQ");
                            if (!targetCol.IsNullable && !targetCol.IsPrimaryKey) constraints.Add("NN");
                            
                            if (constraints.Count > 0)
                            {
                                sb.Append($" [yellow][[{string.Join(",", constraints)}]][/]");
                            }

                            // Dynamic Validation Markers
                            if (validation.IsNullViolation)
                            {
                                sb.Append(" [red bold]❌ NULL VIOLATION[/]");
                            }
                            
                            if (validation.IsLengthViolation)
                            {
                                sb.Append($" [red bold]❌ LEN {validation.ActualLength} > {targetCol.MaxLength}[/]");
                            }

                            if (validation.IsPrecisionViolation)
                            {
                                sb.Append($" [red bold]❌ OVERFLOW {validation.ActualIntegerDigits} > {validation.MaxIntegerDigits} digits[/]");
                            }
                        }
                        else
                        {
                             sb.Append(" -> [red]Missing[/]");
                        }
                    }

                    rowMarkup.Add(sb.ToString());
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

    /// <summary>
    /// Renders the schema compatibility report.
    /// </summary>
    public void RenderCompatibilityReport(SchemaCompatibilityReport report, IAnsiConsole console)
    {
        var table = new Table().Border(TableBorder.Rounded);
        table.Title = new TableTitle("[blue]🔍 Target Schema Compatibility Analysis[/]");

        // Header info
        var headerPanel = BuildHeaderPanel(report);
        console.Write(headerPanel);
        console.WriteLine();

        // Column compatibility table
        table.AddColumn(new TableColumn("Column").Width(20));
        table.AddColumn(new TableColumn("Source Type").Width(15));
        table.AddColumn(new TableColumn("Target Type").Width(20));
        table.AddColumn(new TableColumn("Status").Width(35));

        foreach (var col in report.Columns)
        {
            var sourceType = col.SourceColumn?.ClrType.Name ?? "—";
            var targetType = BuildTargetTypeDisplay(col.TargetColumn);
            var statusDisplay = BuildStatusDisplay(col.Status, col.Message);

            table.AddRow(
                Markup.Escape(col.ColumnName),
                Markup.Escape(sourceType),
                targetType,
                statusDisplay
            );
        }

        console.Write(table);

        // Warnings and Errors
        if (report.Warnings.Count > 0)
        {
            console.WriteLine();
            console.MarkupLine("[yellow]⚠️ Warnings:[/]");
            foreach (var warning in report.Warnings)
            {
                console.MarkupLine($"  [yellow]• {Markup.Escape(warning)}[/]");
            }
        }

        if (report.Errors.Count > 0)
        {
            console.WriteLine();
            console.MarkupLine("[red]❌ Errors:[/]");
            foreach (var error in report.Errors)
            {
                console.MarkupLine($"  [red]• {Markup.Escape(error)}[/]");
            }
        }

        console.WriteLine();
    }

    private Panel BuildHeaderPanel(SchemaCompatibilityReport report)
    {
        var content = new StringBuilder();
        
        if (report.TargetInfo is null || !report.TargetInfo.Exists)
        {
            content.AppendLine("[dim]Target:[/] [green]Will be created (new table)[/]");
        }
        else
        {
            var rowInfo = report.TargetInfo.RowCount.HasValue 
                ? $"{report.TargetInfo.RowCount:N0} rows" 
                : "unknown rows";
            var sizeInfo = report.TargetInfo.SizeBytes.HasValue 
                ? $" • {FormatSize(report.TargetInfo.SizeBytes.Value)}"
                : "";
            
            var statusColor = report.TargetInfo.RowCount > 0 ? "yellow" : "green";
            var statusIcon = report.TargetInfo.RowCount > 0 ? "⚠️" : "✅";
            
            content.AppendLine($"[dim]Target:[/] [{statusColor}]{statusIcon} Table exists ({rowInfo}{sizeInfo})[/]");
            
            if (report.TargetInfo.PrimaryKeyColumns?.Count > 0)
            {
                content.AppendLine($"[dim]Primary Key:[/] {string.Join(", ", report.TargetInfo.PrimaryKeyColumns)}");
            }
        }

        var overallStatus = report.IsCompatible 
            ? "[green]✅ Schema is compatible[/]" 
            : "[red]❌ Schema has compatibility issues[/]";
        content.AppendLine($"[dim]Status:[/] {overallStatus}");

        return new Panel(new Markup(content.ToString().TrimEnd()))
        {
            Border = BoxBorder.Rounded,
            Padding = new Padding(1, 0),
            Header = new PanelHeader("[blue]Target Schema Info[/]")
        };
    }

    private static string BuildTargetTypeDisplay(TargetColumnInfo? targetColumn)
    {
        if (targetColumn is null) return "—";

        var constraints = new List<string>();
        if (targetColumn.IsPrimaryKey) constraints.Add("PK");
        if (!targetColumn.IsNullable && !targetColumn.IsPrimaryKey) constraints.Add("NN");
        if (targetColumn.IsUnique) constraints.Add("UQ");

        var constraintSuffix = constraints.Count > 0 
            ? $" [dim][[{string.Join(",", constraints)}]][/]" 
            : "";

        return $"{Markup.Escape(targetColumn.NativeType)}{constraintSuffix}";
    }

    private static string BuildStatusDisplay(CompatibilityStatus status, string? message)
    {
        return status switch
        {
            CompatibilityStatus.Compatible => "[green]✅ Compatible[/]",
            CompatibilityStatus.WillBeCreated => "[green]✅ Will be created[/]",
            CompatibilityStatus.PossibleTruncation => $"[yellow]⚠️ Possible truncation[/]",
            CompatibilityStatus.TypeMismatch => $"[red]❌ Type mismatch[/]",
            CompatibilityStatus.MissingInTarget => "[red]❌ Missing in target[/]",
            CompatibilityStatus.ExtraInTarget => "[yellow]⚠️ Extra in target[/]",
            CompatibilityStatus.ExtraInTargetNotNull => "[red]❌ Extra (NOT NULL)[/]",
            CompatibilityStatus.NullabilityConflict => "[yellow]⚠️ Nullability conflict[/]",
            _ => "[grey]Unknown[/]"
        };
    }

    private static string FormatSize(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F1} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F1} GB";
    }
}
