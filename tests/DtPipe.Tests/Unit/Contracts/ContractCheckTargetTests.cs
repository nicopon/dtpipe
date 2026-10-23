using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Contracts;
using Xunit;

namespace DtPipe.Tests.Unit.Contracts;

/// <summary>
/// <c>contract check</c> judges a target where the real run reads it, and otherwise as a target that does not exist yet: a sink
/// that has already served gives the verdict it gave before.
/// </summary>
[Collection("console-serial")]
public sealed class ContractCheckTargetTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("contract-check-target-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [Theory]
    [InlineData("csv", "output.csv", "Country,Orders\nFR,3\n")]
    [InlineData("jsonl", "output.jsonl", "{\"Country\":\"FR\",\"Orders\":\"3\"}\n")]
    public async Task A_File_Target_That_Exists_Gives_The_Verdict_Of_One_That_Does_Not(string provider, string file, string content)
    {
        var contract = Path.Combine(_dir, "orders.json");
        var schema = new Schema([new Field("Country", StringType.Default, true), new Field("Orders", Int64Type.Default, false)], null);
        DataContract.FromSchema(schema, "batch", "VerbScanOnly", "main").Write(contract);
        var target = Path.Combine(_dir, file);
        var job = Path.Combine(_dir, "consumer.yaml");
        await File.WriteAllTextAsync(job, $"main:\n  input: generate:1\n  output: {provider}:{target}\n", TestContext.Current.CancellationToken);

        var before = await CheckAsync(job, contract);
        await File.WriteAllTextAsync(target, content, TestContext.Current.CancellationToken);
        var after = await CheckAsync(job, contract);

        Assert.StartsWith("exit 0", before);
        Assert.Contains("will be created", before);
        Assert.Equal(before, after);
    }

    private static async Task<string> CheckAsync(string job, string contract)
    {
        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        try
        {
            var code = await Program.Main(["contract", "check", "--job", job, "--contract", contract]);
            return $"exit {code}\n{captured}";
        }
        finally { Console.SetOut(stdout); Console.SetError(stderr); }
    }
}
