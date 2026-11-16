using DtPipe.Cli;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary><c>@file</c> is a reference to a value, and it reaches the option that owns it on every subcommand.</summary>
public sealed class SubcommandReferenceTests
{
    [Theory]
    [InlineData("studio", "--key")]
    [InlineData("studio", "--llm-key")]
    [InlineData("node", "--client-secret")]
    [InlineData("inspect", "--query")]
    public void An_At_File_Value_Reaches_The_Option_As_Typed(string subcommand, string option)
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        var jobService = services.BuildServiceProvider().GetRequiredService<JobService>();
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "--help\n");
            var parsed = Program.ParseSubcommand(jobService.BuildSubcommands(), [subcommand, option, "@" + file]);
            Assert.Equal("@" + file, parsed.GetValue<string?>(option));
        }
        finally
        {
            File.Delete(file);
        }
    }
}
