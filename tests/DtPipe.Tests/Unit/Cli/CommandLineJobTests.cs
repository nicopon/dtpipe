using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A command line imported as a job file, through the parsing and conversion a run applies: the text
/// <c>--export-job</c> writes, with no variable resolved and no binding carried.
/// </summary>
public sealed class CommandLineJobTests
{
    private static ServiceProvider Services()
    {
        var plain = new ServiceCollection();
        plain.AddLogging();
        plain.AddSingleton<OptionsRegistry>();
        var inner = plain.BuildServiceProvider();
        var options = inner.GetRequiredService<OptionsRegistry>();

        var readers = new IStreamReaderFactory[] { new CliStreamReaderFactory(new DtPipe.Adapters.Csv.CsvReaderDescriptor(), options, inner) };
        var writers = new IDataWriterFactory[] { new CliDataWriterFactory(new DtPipe.Adapters.Csv.CsvWriterDescriptor(), options, inner) };

        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in new PipelineOptionsCliContributor().GetFlagDefs()) registry.Register(def);
        foreach (var f in readers.Cast<IDataFactory>().Concat(writers))
            foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(f.OptionsType)) registry.Register(def);

        var services = new ServiceCollection();
        services.AddSingleton(registry);
        services.AddSingleton<ISecretsManager>(new InMemorySecretsManager());
        services.AddSingleton(readers.AsEnumerable());
        services.AddSingleton(writers.AsEnumerable());
        services.AddSingleton(Enumerable.Empty<IStreamTransformerFactory>());
        services.AddSingleton(Enumerable.Empty<IDataTransformerFactory>());
        return services.BuildServiceProvider();
    }

    [Fact]
    public void A_simple_line_becomes_the_main_branch_of_a_job()
    {
        using var services = Services();
        var yaml = CommandLineJob.Export(services, ["--input", "csv:in.csv", "--output", "csv:out.csv"]);
        Assert.Contains("main:", yaml);
        Assert.Contains("csv:in.csv", yaml);
        Assert.Contains("csv:out.csv", yaml);
    }

    [Fact]
    public void Two_inputs_become_two_branches()
    {
        using var services = Services();
        var yaml = CommandLineJob.Export(services, ["-i", "csv:a.csv", "-o", "csv:x.csv", "-i", "csv:b.csv", "-o", "csv:y.csv"]);
        Assert.Contains("csv:a.csv", yaml);
        Assert.Contains("csv:b.csv", yaml);
        Assert.Contains("csv:x.csv", yaml);
        Assert.Contains("csv:y.csv", yaml);
    }

    [Fact]
    public void A_binding_is_refused_because_it_names_a_location_of_one_run()
    {
        using var services = Services();
        var error = Assert.Throws<ArgumentException>(() =>
            CommandLineJob.Export(services, ["-i", "csv:a.csv", "-o", "csv:x.csv", "--bind-input", "main=/tmp/x.arrows"]));
        Assert.Contains("--bind-input", error.Message);
    }

    [Fact]
    public void A_variable_stays_as_typed_whatever_the_environment_holds()
    {
        Environment.SetEnvironmentVariable("COMMAND_LINE_JOB_DIR", "resolved-by-mistake");
        try
        {
            using var services = Services();
            var yaml = CommandLineJob.Export(services, ["-i", "csv:${{COMMAND_LINE_JOB_DIR}}/in.csv", "-o", "csv:out.csv"]);
            Assert.Contains("${{COMMAND_LINE_JOB_DIR}}", yaml);
            Assert.DoesNotContain("resolved-by-mistake", yaml);
        }
        finally
        {
            Environment.SetEnvironmentVariable("COMMAND_LINE_JOB_DIR", null);
        }
    }
}
