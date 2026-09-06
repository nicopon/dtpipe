using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Reflection;
using System.Text.RegularExpressions;
using DtPipe.Core.Attributes;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Content of <c>get-adapter-help</c>, built over the real component catalog rather than the stub
/// factories used elsewhere: what an MCP client actually receives is the point here.
/// <para>
/// Until these existed the only covered path was the unknown-adapter error, so a dual-role adapter
/// silently emitting just its reader's help went unnoticed. The text these assertions pin is what a
/// model relies on to write a correct job.
/// </para>
/// </summary>
public class McpAdapterHelpTests
{
    private readonly IMcpHelpService _help;
    private readonly string[] _readerNames;

    public McpAdapterHelpTests()
    {
        var catalog = ComponentCatalog.Discover(
            typeof(DtPipe.Program).Assembly,
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
            typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

        var registry = new OptionsRegistry();
        var sp = new ServiceCollection().BuildServiceProvider();

        var readers = catalog.Readers
            .Select(e => (IStreamReaderFactory)new CliStreamReaderFactory(
                (IProviderDescriptor<IStreamReader>)Activator.CreateInstance(e.ImplementationType)!, registry, sp))
            .ToList();
        var writers = catalog.Writers
            .Select(e => (IDataWriterFactory)new CliDataWriterFactory(
                (IProviderDescriptor<IDataWriter>)Activator.CreateInstance(e.ImplementationType)!, registry, sp))
            .ToList();

        _help = new McpHelpService(readers, Array.Empty<IDataTransformerFactory>(), writers);
        _readerNames = readers.Select(r => r.ComponentName).ToArray();

        _roles = readers.Select(r => (Adapter: r.ComponentName, Role: "Reader", r.OptionsType))
            .Concat(writers.Select(w => (Adapter: w.ComponentName, Role: "Writer", w.OptionsType)))
            .ToList();
    }

    private readonly List<(string Adapter, string Role, Type OptionsType)> _roles;

    /// <summary>Adapter names exposing a reader and a writer that both carry help.</summary>
    public static TheoryData<string> DualRoleAdapters()
    {
        var data = new TheoryData<string>();
        foreach (var a in Catalog().GroupBy(x => x.Adapter)
                     // Two distinct options types, not merely two roles: the memory channels
                     // serve both roles from one type, so they yield one unlabelled block.
                     .Where(g => g.Select(x => x.OptionsType).Distinct().Count() > 1
                              && g.All(x => x.OptionsType.GetCustomAttribute<ComponentHelpAttribute>() != null))
                     .Select(g => g.Key))
            data.Add(a);
        return data;
    }

    /// <summary>Adapters whose options descend from DbConnectionOptions — the database providers.</summary>
    public static TheoryData<string> DatabaseAdapters()
    {
        var data = new TheoryData<string>();
        foreach (var a in Catalog()
                     .Where(x => typeof(DtPipe.Adapters.Common.DbConnectionOptions).IsAssignableFrom(x.OptionsType))
                     .Select(x => x.Adapter).Distinct())
            data.Add(a);
        return data;
    }

    /// <summary>
    /// Options types bound to exactly one role, so "the opposite side of the pipeline" is defined.
    /// A type shared by both roles (object storage, memory channels) documents both directions in
    /// its own examples and is excluded.
    /// </summary>
    public static TheoryData<string, string, Type> SingleRoleOptionTypes()
    {
        var data = new TheoryData<string, string, Type>();
        foreach (var g in Catalog().GroupBy(x => x.OptionsType))
        {
            var roles = g.Select(x => x.Role).Distinct().ToList();
            if (roles.Count != 1) continue;
            if (g.Key.GetCustomAttribute<ComponentHelpAttribute>() == null) continue;
            data.Add(g.First().Adapter, roles[0], g.Key);
        }
        return data;
    }

    private static List<(string Adapter, string Role, Type OptionsType)> Catalog()
    {
        var c = ComponentCatalog.Discover(
            typeof(DtPipe.Program).Assembly,
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
            typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

        return c.Readers.Select(e => (((IComponentDescriptor)Activator.CreateInstance(e.ImplementationType)!).ComponentName, "Reader", ((IComponentDescriptor)Activator.CreateInstance(e.ImplementationType)!).OptionsType))
            .Concat(c.Writers.Select(e => (((IComponentDescriptor)Activator.CreateInstance(e.ImplementationType)!).ComponentName, "Writer", ((IComponentDescriptor)Activator.CreateInstance(e.ImplementationType)!).OptionsType)))
            .ToList();
    }

    /// <summary>
    /// Both roles carry their own notes and example. Emitting only the reader's left the writer's
    /// semantics — MySQL's unique-index requirement for upsert, its bulk-load prerequisite —
    /// unreachable, while the writer's options were still listed as if freely usable.
    /// </summary>
    [Theory]
    [MemberData(nameof(DualRoleAdapters))]
    public void DualRoleAdapter_Exposes_Both_Roles(string adapter)
    {
        var help = _help.GetAdapterHelp(adapter);

        Assert.Contains("YAML Usage & Notes (Reader):", help);
        Assert.Contains("YAML Usage & Notes (Writer):", help);
        Assert.Contains("YAML Example Configuration (Reader):", help);
        Assert.Contains("YAML Example Configuration (Writer):", help);
    }

    /// <summary>
    /// The role-suffixed provider-options key is what disambiguates a job reading from and writing
    /// to the same provider, and it is taught only by the writer example.
    /// </summary>
    [Fact]
    public void Writer_Specific_ProviderOptions_Key_Is_Reachable()
        => Assert.Contains("mysql-writer:", _help.GetAdapterHelp("mysql"));

    /// <summary>
    /// A single-role adapter keeps unlabelled sections: the role suffix exists to separate two
    /// blocks, and adding it to a lone one would be noise.
    /// </summary>
    [Fact]
    public void SingleRoleAdapter_Keeps_Unlabelled_Sections()
    {
        var adapter = _roles.GroupBy(x => x.Adapter)
            .First(g => g.Select(x => x.Role).Distinct().Count() == 1
                     && g.Key != "mem" && g.Key != "arrow-memory").Key;

        var help = _help.GetAdapterHelp(adapter);

        Assert.Contains("YAML Usage & Notes:", help);
        Assert.DoesNotContain("(Reader):", help);
    }

    /// <summary>
    /// An example's counterpart side is a placeholder, never a real adapter. Naming one anchors the
    /// model on an unrelated component, and a verbatim copy would silently write a file nobody asked
    /// for — where a placeholder fails closed ("No writer factory resolved for output").
    /// </summary>
    [Theory]
    [MemberData(nameof(SingleRoleOptionTypes))]
    public void Example_Counterpart_Side_Is_A_Placeholder(string adapter, string role, Type optionsType)
    {
        var counterpart = role == "Reader" ? "output" : "input";
        var examples = optionsType.GetCustomAttribute<ComponentHelpAttribute>()!.Examples ?? Array.Empty<string>();

        foreach (var value in examples
                     .SelectMany(e => e.Split('\n'))
                     .Select(l => Regex.Match(l.Trim(), $"^{counterpart}: \"(?<v>[^\"]+)\"$"))
                     .Where(m => m.Success)
                     .Select(m => m.Groups["v"].Value))
        {
            // generate -> null is the one pairing where the counterpart IS the lesson (the
            // throughput idiom); allowed from both ends. Discarding output is not a format choice.
            Assert.True(value.StartsWith('<') || value == "null:" || value.StartsWith("generate:"),
                $"{adapter} ({role}) example names a concrete counterpart '{value}'. Use a placeholder: "
                + "naming a real adapter anchors the model on an unrelated component, and a verbatim "
                + "copy writes a file nobody asked for instead of failing closed.");
        }
    }

    /// <summary>
    /// The example shows the minimum keys, so the help must say the set is open and name the driver
    /// that owns the real vocabulary — for MySQL that also steers away from MySql.Data's options, a
    /// driver this repository deliberately does not ship.
    /// </summary>
    [Theory]
    [MemberData(nameof(DatabaseAdapters))]
    public void Database_Adapter_Names_Its_Driver(string adapter)
    {
        var help = _help.GetAdapterHelp(adapter);

        Assert.Contains("not exhaustive", help);
        Assert.Contains("Driver:", help);
    }

    /// <summary>
    /// A name that is not there is answered with the names that are. A recorded session shows a
    /// model asking for transformer help on a reader it had just read in the list-providers output,
    /// being told only that it did not exist, and spending three more turns rediscovering where it
    /// had seen the name.
    /// </summary>
    [Fact]
    public void An_Unknown_Adapter_Is_Answered_With_The_Ones_That_Exist()
    {
        var json = JsonDocument.Parse(_help.GetAdapterHelp("no-such-adapter")).RootElement;

        Assert.Contains("Unknown adapter", json.GetProperty("error").GetString());
        var available = json.GetProperty("available").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.NotEmpty(available);
        Assert.Equal(_readerNames.OrderBy(n => n, System.StringComparer.Ordinal).First(), available.First());
    }

    /// <summary>
    /// list-providers hands back readers, transformers and writers in one payload, so a name is
    /// easily taken away without its role. Saying which role it does have closes the loop in one
    /// call instead of several.
    /// </summary>
    [Fact]
    public void A_Name_That_Exists_In_Another_Role_Is_Told_So()
    {
        var json = JsonDocument.Parse(_help.GetTransformerHelp(_readerNames[0])).RootElement;

        Assert.Contains("Unknown transformer", json.GetProperty("error").GetString());
        Assert.Contains("is a adapter", json.GetProperty("hint").GetString());
    }
}
