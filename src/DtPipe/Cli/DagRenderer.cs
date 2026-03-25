using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Spectre.Console;

namespace DtPipe.Cli;

internal static class DagRenderer
{
	// ─── Linear Topology Renderer ────────────────────────────────────────────

	internal static Panel BuildLinearTopologyPanel(JobDefinition? job, IEnumerable<IStreamReaderFactory> readerFactories)
	{
		var sb = new System.Text.StringBuilder();
		string input = job?.Input ?? "";

		bool isColumnar = false;
		if (!string.IsNullOrEmpty(input))
		{
			var factory = readerFactories.FirstOrDefault(f =>
				input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
				input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
				f.CanHandle(input));
			isColumnar = factory?.YieldsColumnarOutput == true;
		}

		string modeLabel = isColumnar ? "  [cyan]◈ Arrow[/]" : "  [yellow]● row[/]";
		string inputLabel = !string.IsNullOrEmpty(input) ? $"  [grey]{Markup.Escape(input)}[/]" : string.Empty;

		sb.AppendLine($"  [green]◉[/]{inputLabel}{modeLabel}");

		if (job?.Transformers != null)
			foreach (var t in job.Transformers)
				sb.AppendLine($"     [grey]→ {Markup.Escape(t.Type)}[/]");

		if (!string.IsNullOrEmpty(job?.Output))
			sb.AppendLine($"     [grey]──▶[/]  [blue]{Markup.Escape(job.Output)}[/]");

		return new Panel(new Markup(sb.ToString().TrimEnd()))
			.Header("[yellow] Pipeline [/]")
			.Border(BoxBorder.Rounded)
			.Padding(1, 0);
	}

	// ─── Unified Results Table ────────────────────────────────────────────────

	internal static void PrintUnifiedResultsTable(
		List<DtPipe.Feedback.BranchSummary> results,
		JobDagDefinition dagDefinition,
		bool isDag,
		IAnsiConsole console)
	{
		if (results.Count == 0) return;

		if (isDag)
		{
			var branchOrder = dagDefinition.Branches
				.Select((b, i) => (b.Alias, Index: i))
				.ToDictionary(x => x.Alias, x => x.Index, StringComparer.OrdinalIgnoreCase);
			results = results
				.OrderBy(r => r.Alias != null && branchOrder.TryGetValue(r.Alias, out var idx) ? idx : int.MaxValue)
				.ToList();
		}

		bool hasMode = results.Any(r => r.TransformerModes.Count > 0 || r.ReaderIsColumnar);
		bool hasBranch = isDag && results.Count > 1;

		var table = new Table().Border(TableBorder.Rounded);
		table.Title = new TableTitle("[yellow] Results [/]");

		if (hasBranch) table.AddColumn(new TableColumn("[grey]Branch[/]"));
		table.AddColumn(new TableColumn("[grey]Stage[/]"));
		table.AddColumn(new TableColumn("[grey]Rows[/]").RightAligned());
		table.AddColumn(new TableColumn("[grey]Speed[/]").RightAligned());
		if (hasMode) table.AddColumn(new TableColumn("[grey]Mode[/]"));

		void AddRow(string branch, string stage, string rows, string speed, string mode)
		{
			var cols = new List<string>();
			if (hasBranch) cols.Add(branch);
			cols.Add(stage);
			cols.Add(rows);
			cols.Add(speed);
			if (hasMode) cols.Add(mode);
			table.AddRow(cols.ToArray());
		}

		long totalRows = 0;
		double peakMemory = 0;
		DateTime minStart = DateTime.MaxValue, maxEnd = DateTime.MinValue;
		bool firstBranch = true;

		foreach (var summary in results)
		{
			if (!firstBranch)
				AddRow("", "", "", "", "");
			firstBranch = false;

			var m = summary.Metrics;
			double elapsed = m.Duration.TotalSeconds;
			string branchLabel = summary.Alias != null ? $"[white][[{Markup.Escape(summary.Alias)}]][/]" : "";
			string readMode = summary.ReaderIsColumnar ? "[cyan]◈ Arrow[/]" : "[yellow]● row[/]";

			AddRow(branchLabel, "[grey]▸ Reading[/]", $"[white]{m.ReadCount:N0}[/]", $"[grey]{FormatSpeed(m.ReadCount, elapsed)}[/]", readMode);

			var indexedCounts = m.TransformerCountsByIndex;
			for (int ti = 0; ti < summary.TransformerModes.Count; ti++)
			{
				var (name, isColumnar) = summary.TransformerModes[ti];
				long count = indexedCounts != null && ti < indexedCounts.Count
					? indexedCounts[ti]
					: (m.TransformerStats.TryGetValue(name, out var c) ? c : 0);
				string modeLbl = isColumnar ? "[cyan]◈ columnar[/]" : "[yellow]● row[/]";
				AddRow("", $"[grey]▸ → {Markup.Escape(name)}[/]", $"[white]{count:N0}[/]", $"[grey]{FormatSpeed(count, elapsed)}[/]", modeLbl);
			}

			AddRow("", "[grey]▸ Writing[/]", $"[white]{m.WriteCount:N0}[/]", $"[grey]{FormatSpeed(m.WriteCount, elapsed)}[/]", "");

			totalRows += m.WriteCount;
			if (m.PeakMemoryWorkingSetMb > peakMemory) peakMemory = m.PeakMemoryWorkingSetMb;
			if (m.StartTime < minStart) minStart = m.StartTime;
			if (m.EndTime > maxEnd) maxEnd = m.EndTime;
		}

		console.Write(table);

		double totalElapsed = maxEnd > minStart ? (maxEnd - minStart).TotalSeconds : 0;
		string completionLine = isDag && results.Count > 1
			? $"[green]✓[/] [grey]Total[/]  [white]{totalRows:N0} rows[/] [grey]·  {totalElapsed:F1}s  ·  peak {peakMemory:F0} MB[/]"
			: $"[green]✓[/] [white]{totalRows:N0} rows[/] [grey]·  {totalElapsed:F1}s  ·  peak {peakMemory:F0} MB[/]";
		console.MarkupLine(completionLine);
	}

	internal static string FormatSpeed(long count, double elapsedSeconds)
	{
		double rps = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
		return rps switch
		{
			>= 1_000_000 => $"{rps / 1_000_000:F1}M/s",
			>= 1_000 => $"{rps / 1_000:F1}K/s",
			_ => $"{rps:F0}/s"
		};
	}

	// ─── DAG Topology Renderer ───────────────────────────────────────────────

	internal static Panel BuildTopologyPanel(JobDagDefinition dag, IEnumerable<IStreamReaderFactory> readerFactories)
	{
		var readerFactoryList = readerFactories.ToList();
		var lines = new System.Text.StringBuilder();
		bool first = true;

		foreach (var branch in dag.Branches)
		{
			if (!first) lines.AppendLine();
			first = false;

			if (branch.HasStreamTransformer)
			{
				var fromPart = branch.StreamingAliases.Count > 0
					? $"  [grey]← [[{Markup.Escape(branch.StreamingAliases[0])}]][/]"
					: string.Empty;
				var refPart = branch.RefAliases.Any()
					? $"  [grey]+ref {string.Join(", ", branch.RefAliases.Select(r => $"[[{Markup.Escape(r)}]]"))}[/]"
					: string.Empty;
				var extraStreamsPart = branch.StreamingAliases.Count > 1
					? $"  [grey]+from {string.Join(", ", branch.StreamingAliases.Skip(1).Select(m => $"[[{Markup.Escape(m)}]]"))}[/]"
					: string.Empty;

				lines.AppendLine($" [cyan]⚡[/] [white][[{Markup.Escape(branch.Alias)}]][/]{fromPart}{refPart}{extraStreamsPart}");

				if (branch.ProcessorName == "sql")
				{
					var sql = (DtPipe.Cli.Dag.CliDagParser.ExtractArgValue(branch.Arguments, "--sql") ?? string.Empty).Trim().Replace('\n', ' ').Replace('\r', ' ');
					while (sql.Contains("  ")) sql = sql.Replace("  ", " ");
					if (sql.Length > 60) sql = sql[..57] + "...";
					lines.AppendLine($"      [grey]SQL › {Markup.Escape(sql)}[/]");
				}
				else
				{
					lines.AppendLine($"      [grey]{branch.ProcessorName}[/]");
				}

				AppendTransformers(lines, branch, "      ");

				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
			else if (branch.StreamingAliases.Count > 0)
			{
				lines.AppendLine($"  [green]◉[/] [white][[{Markup.Escape(branch.Alias)}]][/]  [grey]← [[{Markup.Escape(branch.StreamingAliases[0])}]][/]");
				AppendTransformers(lines, branch, "      ");
				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
			else
			{
				bool isArrow = TopologyIsArrowChannel(dag, branch.Alias, readerFactoryList);
				bool feedsChannel = string.IsNullOrEmpty(branch.Output);

				var consumers = dag.Branches
					.Where(b => b != branch && (
						b.RefAliases.Contains(branch.Alias, StringComparer.OrdinalIgnoreCase) ||
						b.StreamingAliases.Contains(branch.Alias, StringComparer.OrdinalIgnoreCase)))
					.Select(b => $"[[{Markup.Escape(b.Alias)}]]")
					.ToList();

				string downstreamLabel = consumers.Count > 0 && feedsChannel
					? $"  [grey]→  {string.Join(", ", consumers)}[/]"
					: string.Empty;

				lines.AppendLine($"  [green]◉[/] [white][[{Markup.Escape(branch.Alias)}]][/]{downstreamLabel}");

				if (!string.IsNullOrEmpty(branch.Input))
				{
					string modeLabel = feedsChannel
						? (isArrow ? "  [cyan]◈ Arrow[/]" : "  [yellow]● row[/]")
						: string.Empty;
					lines.AppendLine($"      [grey]← {Markup.Escape(branch.Input)}[/]{modeLabel}");
				}

				AppendTransformers(lines, branch, "      ");

				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
		}

		var content = lines.ToString().TrimEnd();
		return new Panel(new Markup(content))
			.Header("[yellow] Pipeline [/]")
			.Border(BoxBorder.Rounded)
			.Padding(1, 0);
	}

	internal static bool TopologyIsArrowChannel(JobDagDefinition dag, string alias, List<IStreamReaderFactory> readerFactories)
	{
		if (dag.Branches.Any(b => b.HasStreamTransformer && (
			b.RefAliases.Contains(alias, StringComparer.OrdinalIgnoreCase) ||
			b.StreamingAliases.Contains(alias, StringComparer.OrdinalIgnoreCase))))
			return true;

		var producer = dag.Branches.FirstOrDefault(b =>
			b.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase) && !b.HasStreamTransformer);
		if (producer != null && !string.IsNullOrEmpty(producer.Input))
		{
			var factory = readerFactories.FirstOrDefault(f =>
				producer.Input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
				producer.Input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
				f.CanHandle(producer.Input));
			if (factory?.YieldsColumnarOutput == true) return true;
		}
		return false;
	}

	internal static void AppendTransformers(System.Text.StringBuilder sb, BranchDefinition branch, string indent)
	{
		var transformers = branch.PreParsedJob?.Transformers;
		if (transformers?.Any() != true) return;
		foreach (var t in transformers)
			sb.AppendLine($"{indent}[grey]→ {Markup.Escape(t.Type)}[/]");
	}
}
