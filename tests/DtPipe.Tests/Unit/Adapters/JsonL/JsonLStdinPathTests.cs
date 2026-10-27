using System.Diagnostics;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

/// <summary>
/// '--path' over stdin loads the whole input once into memory and streams the array from that
/// copy. A reader-level test cannot exercise this: Console.OpenStandardInput() opens the OS stream
/// directly and ignores Console.SetIn, so this spawns the real CLI with its stdin redirected.
/// </summary>
public class JsonLStdinPathTests
{
    [Fact]
    public async Task A_Max_Sample_Below_The_Target_Arrays_Size_Still_Yields_Every_Element_From_Stdin()
    {
        var dtpipeDll = Path.Combine(AppContext.BaseDirectory, "DtPipe.dll");
        var target = Path.Combine(Path.GetTempPath(), $"jsonl_stdin_{Guid.NewGuid()}.jsonl");

        try
        {
            var psi = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            foreach (var arg in new[]
            {
                dtpipeDll, "-i", "jsonl:-", "--path", "items", "--max-sample", "2",
                "-o", $"jsonl:{target}", "--no-stats"
            })
                psi.ArgumentList.Add(arg);

            using var process = Process.Start(psi)!;
            await process.StandardInput.WriteAsync(
                """{"items":[{"id":1,"v":"a"},{"id":2,"v":"b"},{"id":3,"v":"c"},{"id":4,"v":"d"},{"id":5,"v":"e"}]}""");
            process.StandardInput.Close();

            var stderr = await process.StandardError.ReadToEndAsync();
            await process.WaitForExitAsync();
            process.ExitCode.Should().Be(0, stderr);

            var lines = await File.ReadAllLinesAsync(target);
            lines.Should().HaveCount(5);
        }
        finally
        {
            try { File.Delete(target); } catch { /* best effort */ }
        }
    }
}
