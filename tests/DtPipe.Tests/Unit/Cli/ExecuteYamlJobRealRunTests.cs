using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// execute-yaml-job with <c>apply=true</c> and an approving gate: the run goes through the engine, so
/// what a failed branch answers has to reach the model that asked for it.
/// </summary>
[Collection("console-serial")]
public class ExecuteYamlJobRealRunTests
{
    private static DtPipeMcpTools BuildTools()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IApprovalGate>(new DefaultApprovalGate(_ => true));   // last registration wins
        return services.BuildServiceProvider().GetRequiredService<DtPipeMcpTools>();
    }

    [Fact]
    public async Task A_Branch_That_Fails_Without_Throwing_Is_Named_With_Its_Reason()
    {
        var tools = BuildTools();
        var missing = Path.Combine(Path.GetTempPath(), $"dtpipe-absent-{Guid.NewGuid():N}.csv");
        var yaml = $"main:\n  input: \"csv:{missing}\"\n  output: \"null:\"\n";

        var json = await tools.ExecuteYamlJob(yaml, apply: true);

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.False(root.GetProperty("success").GetBoolean());
        Assert.True(root.GetProperty("applied").GetBoolean());
        var error = root.GetProperty("error").GetString();
        Assert.StartsWith("Branch 'main': ", error);
        Assert.Contains("dtpipe-absent", error);
    }
}
