using System.CommandLine;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Linq;
using System.Text;

namespace DtPipe.Cli.Commands;

public class EngineDuckDbCommand : Command
{
    public EngineDuckDbCommand() : base("engine-duckdb", "[HIDDEN] Internal DuckDB engine for isolated execution")
    {
        var queryOption = new Option<string>("--query") { Description = "SQL query to execute" };
        var inOption = new Option<string[]>("--in") { AllowMultipleArgumentsPerToken = true, Description = "Input sources in alias=path format" };

        Options.Add(queryOption);
        Options.Add(inOption);

        this.SetAction(async (parseResult, ct) =>
        {
            var query = parseResult.GetValue(queryOption) ?? "";
            var inputs = parseResult.GetValue(inOption) ?? Array.Empty<string>();

            if (string.IsNullOrEmpty(query)) return;

            await ExecuteAsync(query, inputs, ct);
        });
    }

    private async Task ExecuteAsync(string query, string[] inputs, CancellationToken ct)
    {
        inputs ??= Array.Empty<string>();
        Console.Error.WriteLine($"[ENGINE] {DateTime.Now:HH:mm:ss.fff} Starting ExecuteAsync. Query={query}, Inputs={inputs.Length}");

        using var connection = new DuckDBConnection("DataSource=:memory:");
        await connection.OpenAsync(ct);

        // 1. Load Arrow extension
        var extensionPath = await DtPipe.Cli.Infrastructure.ExtensionManager.GetExtensionPathAsync("arrow");

        using (var cmd = connection.CreateCommand())
        {
            try
            {
                if (!string.IsNullOrEmpty(extensionPath))
                {
                    var escapedPath = extensionPath.Replace("'", "''");
                    cmd.CommandText = $"INSTALL '{escapedPath}'; LOAD arrow;";
                    await cmd.ExecuteNonQueryAsync(ct);
                    Console.Error.WriteLine($"[ENGINE] Loaded arrow extension from: {extensionPath}");
                }
                else
                {
                    cmd.CommandText = "INSTALL arrow; LOAD arrow;";
                    await cmd.ExecuteNonQueryAsync(ct);
                    Console.Error.WriteLine("[ENGINE] Loaded arrow extension from community.");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ENGINE] Warning: Extension load failed: {ex.Message}");
            }
        }

        // 2. Prepare CTE Wrapper for Input Views
        // We use CTEs instead of CREATE VIEW because CREATE VIEW forces DuckDB to read the header
        // of an Arrow stream to infer its schema. If it reads the header from a FIFO/stdin stream,
        // the subsequent query execution will fail with 'not enough data' because the header is gone.
        var sb = new StringBuilder();
        var validInputs = inputs.Select(i => i.Split('=', 2)).Where(p => p.Length == 2).ToList();

        if (validInputs.Count > 0)
        {
            sb.AppendLine("WITH");
            for (int i = 0; i < validInputs.Count; i++)
            {
                var alias = validInputs[i][0];
                var path = validInputs[i][1];
                sb.Append($"\"{alias}\" AS (SELECT * FROM read_arrow('{path}'))");
                if (i < validInputs.Count - 1) sb.AppendLine(",");
                else sb.AppendLine();
            }
            sb.AppendLine($"SELECT * FROM ({query})");
        }
        else
        {
            sb.AppendLine(query);
        }

        var finalQuery = sb.ToString();

        // 3. Execute Query and Stream to Stdout in Arrow IPC format
        using (var cmd = connection.CreateCommand())
        {
            // We use 'FORMAT ARROWS' for streaming IPC
            string stdoutPath = OperatingSystem.IsWindows() ? "stdout" : "/dev/stdout";
            cmd.CommandText = $"COPY ({finalQuery}) TO '{stdoutPath}' (FORMAT ARROWS)";
            Console.Error.WriteLine($"[ENGINE] Executing COPY to {stdoutPath}");

            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
                Console.Error.WriteLine("[ENGINE] Execution complete.");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ENGINE] Execution failed: {ex.Message}");
                Console.Error.WriteLine($"[ENGINE] Final Query: {finalQuery}");
                throw;
            }
        }
    }
}
