using System.Diagnostics;

namespace ArrowSocketSource;

/// <summary>
/// Step one, with no coordinator: dtpipe reads the service's sockets as any reader would, and each case is judged on the exit
/// code and on the rows it produced, never on the exit code alone.
/// </summary>
public sealed class Probe(string dtpipe, string orders, string failing, string nobody, FakeOrdersApi api)
{
    private readonly List<(string Case, bool Passed, string Detail)> _results = [];

    public async Task<int> RunAsync()
    {
        var expected = await ExpectedRowsAsync();
        Console.WriteLine($"Expected rows: {expected} (of {api.TotalOrders} orders, test orders dropped)");

        var full = await ReadAsync(orders);
        Judge("full read", full.Code == 0 && full.Rows == expected, full, $"rows {full.Rows}/{expected}");

        var before = api.PagesFetched;
        var limited = await ReadAsync(orders, "--limit", "5");
        Judge("--limit 5", limited.Code == 0 && limited.Rows == 5, limited, $"rows {limited.Rows}/5");
        // The producer stops when the reader leaves: give it the moment a write needs to fail, then count what it fetched.
        await Task.Delay(500);
        var fetched = api.PagesFetched - before;
        Judge("--limit 5 stops the external API", fetched < PageCount(), limited, $"{fetched}/{PageCount()} pages fetched");

        var truncated = await ReadAsync(failing);
        Judge("a failing producer fails the run", truncated.Code != 0 && truncated.Stderr.Contains("truncated", StringComparison.OrdinalIgnoreCase),
            truncated, $"exit {truncated.Code}, rows written {truncated.Rows}");

        var parallel = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => ReadAsync(orders)));
        Judge("3 concurrent reads", parallel.All(r => r.Code == 0 && r.Rows == expected), parallel[0],
            string.Join(", ", parallel.Select(r => $"exit {r.Code} rows {r.Rows}")));

        var again = await ReadAsync(orders);
        Judge("read after a failure and concurrent ones", again.Code == 0 && again.Rows == expected, again, $"rows {again.Rows}/{expected}");

        var watch = Stopwatch.StartNew();
        var none = await ReadAsync(nobody);
        Judge("nothing listening fails", none.Code != 0, none, $"exit {none.Code} after {watch.Elapsed.TotalSeconds:F1}s");

        Console.WriteLine();
        foreach (var (name, passed, detail) in _results)
            Console.WriteLine($"{(passed ? "PASS" : "FAIL")} {name,-40} {detail}");
        return _results.All(r => r.Passed) ? 0 : 1;
    }

    private int PageCount() => (int)Math.Ceiling(api.TotalOrders / 500.0);

    private async Task<long> ExpectedRowsAsync()
    {
        long rows = 0;
        await foreach (var page in api.FetchPagesAsync(CancellationToken.None, delay: false))
            rows += page.Count(OrdersTransform.IsKept);
        return rows;
    }

    private void Judge(string name, bool passed, Result result, string detail)
    {
        _results.Add((name, passed, detail));
        if (!passed && result.Stderr.Length > 0)
            Console.WriteLine($"  {name}: stderr tail:\n    {string.Join("\n    ", result.Stderr.Split('\n').Where(l => l.Length > 0).TakeLast(4))}");
        else if (result.Stderr.Length > 0 && result.Code != 0)
            Console.WriteLine($"  {name}: dtpipe says: {result.Stderr.Split('\n').Where(l => l.Length > 0).LastOrDefault()}");
    }

    private sealed record Result(int Code, long Rows, string Stderr);

    private async Task<Result> ReadAsync(string socket, params string[] extra)
    {
        // The tool package's entry point is a dll, which runs through the dotnet host.
        var managed = dtpipe.EndsWith(".dll", StringComparison.OrdinalIgnoreCase);
        var psi = new ProcessStartInfo
        {
            FileName = managed ? "dotnet" : dtpipe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        if (managed) psi.ArgumentList.Add(dtpipe);
        foreach (var arg in new[] { "-i", "arrow:pipe://" + socket, "-o", "csv:-", "--no-stats" }.Concat(extra))
            psi.ArgumentList.Add(arg);
        psi.Environment["NO_COLOR"] = "1";
        psi.Environment["DTPIPE_NO_TUI"] = "1";
        using var process = Process.Start(psi)!;
        var stderr = process.StandardError.ReadToEndAsync();
        long lines = 0;
        while (await process.StandardOutput.ReadLineAsync() is { } line)
            if (line.Length > 0) lines++;
        await process.WaitForExitAsync();
        // The first line is the CSV header.
        return new Result(process.ExitCode, Math.Max(0, lines - 1), await stderr);
    }
}
