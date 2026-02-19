using System.Text;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DtPipe.DryRun;
using Spectre.Console;

namespace DtPipe.Cli.DryRun;

public class DryRunRenderer
{
	#region Style Constants
	private const string ColorOutput = "blue";
	private const string ColorNewValue = "green";
	private const string ColorModifiedValue = "yellow";
	private const string ColorStep = "yellow";
	private const string ColorNull = "grey";
	private const string ColorDim = "dim";
	private const string ColorWarning = "yellow";

	private const string OutputHeaderNormal = "Output";
	private const string OutputWarningEmoji = "⚠️";
	private const string OutputWarningSuffix = OutputWarningEmoji + " unverified";
	private const string OutputHeaderWithWarningText = OutputHeaderNormal + " (" + OutputWarningSuffix + ")";
	#endregion

	public int[] CalculateMaxWidths(
		List<SampleTrace> samples,
		List<string> stepNames,
		bool hasSchemaWarning = false,
		TargetSchemaInfo? targetSchema = null)
	{
		if (samples.Count == 0) return Array.Empty<int>();

		int columnCount = 2 + stepNames.Count + 1;
		var maxWidths = new int[columnCount];

		maxWidths[0] = "Column".Length;
		maxWidths[1] = "Input".Length;
		for (int s = 0; s < stepNames.Count; s++)
		{
			maxWidths[2 + s] = Math.Max(maxWidths[2 + s], stepNames[s].Length + 10);
		}
		var outputHeaderText = hasSchemaWarning ? OutputHeaderWithWarningText : OutputHeaderNormal;
		maxWidths[columnCount - 1] = outputHeaderText.Length;

		foreach (var trace in samples)
		{
			var finalStage = trace.Stages.Last();
			var finalSchema = finalStage.Schema;

			foreach (var col in finalSchema)
			{
				maxWidths[0] = Math.Max(maxWidths[0], col.Name.Length);

				for (int stageIdx = 0; stageIdx < trace.Stages.Count; stageIdx++)
				{
					var stage = trace.Stages[stageIdx];
					var schema = stage.Schema;
					var values = stage.Values;

					int idx = -1;
					for (int k = 0; k < schema.Count; k++) if (schema[k].Name == col.Name) { idx = k; break; }

					if (values != null && idx >= 0 && idx < values.Length)
					{
						var v = values[idx];
						var displayLen = (v?.ToString() ?? "null").Length + (v?.GetType().Name ?? "null").Length + 3;
						maxWidths[1 + stageIdx] = Math.Max(maxWidths[1 + stageIdx], displayLen);
					}
				}

				int finalIdx = -1;
				for (int k = 0; k < finalSchema.Count; k++) if (finalSchema[k].Name == col.Name) { finalIdx = k; break; }

				if (finalIdx >= 0 && finalStage.Values != null)
				{
					var displayLen = finalSchema[finalIdx].ClrType.Name.Length + 5;
					if (targetSchema != null) displayLen += 30;
					maxWidths[columnCount - 1] = Math.Max(maxWidths[columnCount - 1], displayLen);
				}
			}
		}

		for (int i = 0; i < maxWidths.Length; i++) maxWidths[i] = Math.Min(maxWidths[i] + 2, 50);

		return maxWidths;
	}

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
		table.Title = new TableTitle(totalSamples > 1
			? $"[green]Pipeline Trace Analysis[/] — Record {sampleIndex + 1}/{totalSamples}"
			: "[green]Pipeline Trace Analysis[/]");

		int colIdx = 0;

		var colNameColumn = new TableColumn("Column");
		if (columnWidths != null) colNameColumn.Width = columnWidths[colIdx++];
		table.AddColumn(colNameColumn);

		var inputColumn = new TableColumn("Input");
		if (columnWidths != null) inputColumn.Width = columnWidths[colIdx++];
		table.AddColumn(inputColumn);

		for (int s = 0; s < stepNames.Count; s++)
		{
			var stepColumn = new TableColumn($"[{ColorStep}]{stepNames[s]} (Step {s + 1})[/]");
			if (columnWidths != null) stepColumn.Width = columnWidths[colIdx++];
			table.AddColumn(stepColumn);
		}

		var outputHeader = string.IsNullOrEmpty(schemaWarning)
			? $"[{ColorOutput}]{OutputHeaderNormal}[/]"
			: $"[{ColorOutput}]{OutputHeaderNormal}[/] [{ColorDim}]({OutputWarningSuffix})[/]";
		var outputColumn = new TableColumn(outputHeader);
		if (columnWidths != null) outputColumn.Width = columnWidths[colIdx];
		table.AddColumn(outputColumn);

		var finalStage = trace.Stages.Last();
		var finalSchema = finalStage.Schema;

		foreach (var col in finalSchema)
		{
			var rowMarkup = new List<string> { Markup.Escape(col.Name) };
			string lastValue = "";

			for (int stageIdx = 0; stageIdx < trace.Stages.Count; stageIdx++)
			{
				var stage = trace.Stages[stageIdx];
				var schema = stage.Schema;
				var values = stage.Values;

				int idx = -1;
				for (int k = 0; k < schema.Count; k++) if (schema[k].Name == col.Name) { idx = k; break; }

				string displayVal;
				string rawVal;

				if (values == null || idx == -1 || idx >= values.Length)
				{
					displayVal = values == null ? "[dim]Filtered[/]" : "";
					rawVal = "N/A_NOT_EXIST";
				}
				else
				{
					var v = values[idx];
					rawVal = v?.ToString() ?? "";
					var typeSuffix = $" [{ColorDim}]({Markup.Escape(v?.GetType().Name ?? "null")})[/]";
					displayVal = v is null ? $"[{ColorNull}]null[/]" : Markup.Escape(rawVal) + typeSuffix;
				}

				if (stageIdx > 0 && rawVal != "N/A_NOT_EXIST")
				{
					bool isNew = idx != -1 && lastValue == "N/A_NOT_EXIST";
					bool isMod = idx != -1 && !isNew && rawVal != lastValue;

					if (isNew) displayVal = $"[{ColorNewValue}]{displayVal}[/]";
					else if (isMod) displayVal = $"[{ColorModifiedValue}]{displayVal}[/]";

					lastValue = idx != -1 ? rawVal : "N/A_NOT_EXIST";
				}
				else
				{
					lastValue = idx != -1 ? rawVal : "N/A_NOT_EXIST";
				}

				rowMarkup.Add(displayVal);
			}

			// Final Output Column
			{
				int finalIdx = -1;
				for (int k = 0; k < finalSchema.Count; k++) if (finalSchema[k].Name == col.Name) { finalIdx = k; break; }

				if (finalIdx >= 0 && finalStage.Values != null)
				{
					var v = finalStage.Values[finalIdx];
					var colName = finalSchema[finalIdx].Name;
					var typeName = finalSchema[finalIdx].ClrType.Name;

					var sb = new StringBuilder();
					sb.Append($"[dim]{Markup.Escape(typeName)}[/]");

					if (targetSchema != null)
					{
						var targetCol = targetSchema.Columns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
						if (targetCol != null)
						{
							var validation = SchemaValidator.Validate(v, targetCol);
							sb.Append(" -> ");

							var nativeTypeMarkup = validation.HasAnyViolation
								? $"[red]{Markup.Escape(targetCol.NativeType)}[/]"
								: $"[blue]{Markup.Escape(targetCol.NativeType)}[/]";
							sb.Append(nativeTypeMarkup);

							var constraints = new List<string>();
							if (targetCol.IsPrimaryKey) constraints.Add("PK");
							if (targetCol.IsUnique) constraints.Add("UQ");
							if (!targetCol.IsNullable && !targetCol.IsPrimaryKey) constraints.Add("NN");

							if (constraints.Count > 0) sb.Append($" [yellow][[{string.Join(",", constraints)}]][/]");

							if (validation.IsNullViolation) sb.Append(" [red bold]❌ NULL VIOLATION[/]");
							if (validation.IsLengthViolation) sb.Append($" [red bold]❌ LEN {validation.ActualLength} > {targetCol.MaxLength}[/]");
							if (validation.IsPrecisionViolation) sb.Append($" [red bold]❌ OVERFLOW {validation.ActualIntegerDigits} > {validation.MaxIntegerDigits} digits[/]");
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

	public void RenderCompatibilityReport(SchemaCompatibilityReport report, IAnsiConsole console)
	{
		var table = new Table().Border(TableBorder.Rounded);
		table.Title = new TableTitle("[blue]🔍 Target Schema Compatibility Analysis[/]");

		console.Write(BuildHeaderPanel(report));
		console.WriteLine();

		table.AddColumn(new TableColumn("Column").Width(20));
		table.AddColumn(new TableColumn("Source Type").Width(15));
		table.AddColumn(new TableColumn("Target Type").Width(20));
		table.AddColumn(new TableColumn("Status").Width(35));

		foreach (var col in report.Columns)
		{
			table.AddRow(
				Markup.Escape(col.ColumnName),
				Markup.Escape(col.SourceColumn?.ClrType.Name ?? "—"),
				BuildTargetTypeDisplay(col.TargetColumn),
				BuildStatusDisplay(col.Status, col.Message)
			);
		}

		console.Write(table);

		if (report.Warnings.Count > 0)
		{
			console.WriteLine();
			console.MarkupLine("[yellow]⚠️ Warnings:[/]");
			foreach (var warning in report.Warnings) console.MarkupLine($"  [yellow]• {Markup.Escape(warning)}[/]");
		}

		if (report.Errors.Count > 0)
		{
			console.WriteLine();
			console.MarkupLine("[red]❌ Errors:[/]");
			foreach (var error in report.Errors) console.MarkupLine($"  [red]• {Markup.Escape(error)}[/]");
		}

		console.WriteLine();
	}

	public void RenderKeyValidation(KeyValidationResult? validation, IAnsiConsole console)
	{
		if (validation == null) return;

		var content = new StringBuilder();
		if (!validation.IsRequired)
		{
			content.AppendLine("[dim]Key requirement:[/] [grey]Not required for this strategy[/]");
		}
		else
		{
			string reqStatus = !validation.IsValid ? "[red]❌ Required[/]" : (validation.Warnings?.Count > 0 ? "[yellow]⚠️ Required (with warnings)[/]" : "[green]✅ Required[/]");
			content.AppendLine($"[dim]Key requirement:[/] {reqStatus}");

			if (validation.RequestedKeys?.Count > 0) content.AppendLine($"[dim]Requested keys:[/] {string.Join(", ", validation.RequestedKeys)}");
			else content.AppendLine($"[dim]Requested keys:[/] [red italic]None specified (use --key option)[/]");

			if (validation.ResolvedKeys?.Count > 0)
			{
				var color = validation.IsValid ? "green" : "yellow";
				content.AppendLine($"[dim]Resolved keys:[/] [{color}]{string.Join(", ", validation.ResolvedKeys)}[/]");
			}

			if (validation.TargetPrimaryKeys?.Count > 0) content.AppendLine($"[dim]Target PK columns:[/] [blue]{string.Join(", ", validation.TargetPrimaryKeys)}[/]");
		}

		if (validation.Warnings?.Count > 0)
		{
			content.AppendLine();
			content.AppendLine("[yellow bold]Warnings:[/]");
			foreach (var warning in validation.Warnings) content.AppendLine($"[yellow]  • {Markup.Escape(warning)}[/]");
		}

		if (validation.Errors?.Count > 0)
		{
			content.AppendLine();
			content.AppendLine("[red bold]Errors:[/]");
			foreach (var error in validation.Errors) content.AppendLine($"[red]  • {Markup.Escape(error)}[/]");
		}

		var panelStyle = validation.IsValid ? (validation.Warnings?.Count > 0 ? "yellow" : "green") : "red";
		var panelIcon = validation.IsValid ? (validation.Warnings?.Count > 0 ? "⚠️" : "🔑") : "❌";

		var panel = new Panel(new Markup(content.ToString().TrimEnd()))
		{
			Border = BoxBorder.Rounded,
			Padding = new Padding(1, 0),
			Header = new PanelHeader($"[{panelStyle}]{panelIcon} Primary Key Validation[/]")
		};

		console.Write(panel);
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
			var estimateSuffix = report.TargetInfo.IsRowCountEstimate ? " [dim](estimate)[/]" : "";
			var rowInfo = report.TargetInfo.RowCount.HasValue ? $"{report.TargetInfo.RowCount:N0} rows{estimateSuffix}" : "unknown rows";
			var sizeInfo = report.TargetInfo.SizeBytes.HasValue ? $" • {FormatSize(report.TargetInfo.SizeBytes.Value)}" : "";
			var statusColor = report.TargetInfo.RowCount > 0 ? "yellow" : "green";
			var statusIcon = report.TargetInfo.RowCount > 0 ? "⚠️" : "✅";

			content.AppendLine($"[dim]Target:[/] [{statusColor}]{statusIcon} Table exists ({rowInfo}{sizeInfo})[/]");
			if (report.TargetInfo.PrimaryKeyColumns?.Count > 0) content.AppendLine($"[dim]Primary Key:[/] {string.Join(", ", report.TargetInfo.PrimaryKeyColumns)}");
		}

		content.AppendLine($"[dim]Status:[/] {(report.IsCompatible ? "[green]✅ Schema is compatible[/]" : "[red]❌ Schema has compatibility issues[/]")}");

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
		var constraintSuffix = constraints.Count > 0 ? $" [dim][[{string.Join(",", constraints)}]][/]" : "";
		return $"{Markup.Escape(targetColumn.NativeType)}{constraintSuffix}";
	}

	private static string BuildStatusDisplay(CompatibilityStatus status, string? message)
	{
		return status switch
		{
			CompatibilityStatus.Compatible => "[green]✅ Compatible[/]",
			CompatibilityStatus.WillBeCreated => "[green]✅ Will be created[/]",
			CompatibilityStatus.PossibleTruncation => "[yellow]⚠️ Possible truncation[/]",
			CompatibilityStatus.TypeMismatch => "[red]❌ Type mismatch[/]",
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

	public void RenderConstraintValidation(ConstraintValidationResult? validation, IAnsiConsole console)
	{
		if (validation == null || validation.IsValid) return;

		var content = new StringBuilder();
		if (validation.Warnings?.Count > 0)
		{
			content.AppendLine("[yellow bold]⚠️ Constraint Warnings:[/]");
			foreach (var warning in validation.Warnings) content.AppendLine($"[yellow]  • {Markup.Escape(warning)}[/]");
		}

		if (validation.Errors?.Count > 0)
		{
			if (validation.Warnings?.Count > 0) content.AppendLine();
			content.AppendLine("[red bold]❌ Data Violations:[/]");
			foreach (var error in validation.Errors) content.AppendLine($"[red]  • {Markup.Escape(error)}[/]");
		}

		var panelStyle = validation.Errors?.Count > 0 ? "red" : "yellow";
		var panelHeader = validation.Errors?.Count > 0 ? "[red]❌ Data Constraint Violations[/]" : "[yellow]⚠️ Data Constraint Warnings[/]";

		var panel = new Panel(new Markup(content.ToString().TrimEnd()))
		{
			Border = BoxBorder.Rounded,
			Padding = new Padding(1, 0),
			Header = new PanelHeader(panelHeader)
		};

		console.Write(panel);
		console.WriteLine();
	}

	public void RenderPerformanceHints(IReadOnlyDictionary<string, string> hints, IAnsiConsole console)
	{
		if (hints == null || hints.Count == 0) return;

		var content = new StringBuilder();
		content.AppendLine("[yellow]Dynamic JS transformers generate implicit 'string' types by default. To optimize mapping overhead, provide strict types:[/]");
		content.AppendLine();

		var options = string.Join(" ", hints.Select(kv => $"--compute-types {Markup.Escape(kv.Key)}:{kv.Value}"));

		content.AppendLine($"  [cyan]dtpipe ... {options}[/]");

		var panel = new Panel(new Markup(content.ToString().TrimEnd()))
		{
			Border = BoxBorder.Rounded,
			Padding = new Padding(1, 0),
			Header = new PanelHeader("[yellow]⚡ Performance Hints[/]")
		};

		console.Write(panel);
		console.WriteLine();
	}
}
