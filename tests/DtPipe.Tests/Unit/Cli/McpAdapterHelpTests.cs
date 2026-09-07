using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Reflection;
using System.Text.RegularExpressions;
using DtPipe.Core.Attributes;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Pipeline;
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
public partial class McpAdapterHelpTests
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
    /// A closed set of values is printed, not described. sqlite's write strategy was documented as
    /// three of its six members while the example beside it used a fourth, so Upsert,
    /// DeleteThenInsert and Ignore were invisible to a caller choosing one. Reading the members off
    /// the enum removes the class of drift rather than one instance of it.
    /// </summary>
    [Fact]
    public void An_Enum_Option_Shows_Its_Members_Instead_Of_A_Placeholder()
    {
        foreach (var (adapter, _, optionsType) in _roles)
        {
            var help = _help.GetAdapterHelp(adapter);
            foreach (var prop in optionsType.GetProperties().Where(p => p.CanWrite))
            {
                var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (!type.IsEnum) continue;

                var expected = $"{prop.Name.ToKebabCase()}: {string.Join(" | ", Enum.GetNames(type))}";
                Assert.Contains(expected, help);
            }
        }
    }

    /// <summary>
    /// Every option the help lists carries an explanation. Listing every public settable property
    /// published six keys with none — among them DuckDB's 'variant', which the router sets from the
    /// 'duck+mysql:' selector and a caller must never supply.
    /// </summary>
    [Fact]
    public void No_Option_Is_Listed_Without_Saying_What_It_Does()
    {
        foreach (var (adapter, _, _) in _roles)
        {
            var lines = _help.GetAdapterHelp(adapter).Split('\n');
            for (int i = 0; i < lines.Length; i++)
            {
                if (!OptionLine().IsMatch(lines[i])) continue;
                var next = i + 1 < lines.Length ? lines[i + 1].Trim() : string.Empty;
                Assert.True(next.StartsWith('#'),
                    $"{adapter}: '{lines[i].Trim()}' is listed with no explanation");
            }
        }
    }

    /// <summary>An option line inside a listing: indented, "name: placeholder", never a YAML
    /// example (which is flush left or nested under a branch alias).</summary>
    [GeneratedRegex(@"^    [a-z0-9-]+: (<value>|true \| false|[A-Za-z]+( \| [A-Za-z]+)+)$")]
    private static partial Regex OptionLine();

    /// <summary>An example must set an option to a value that option accepts. sqlite's used
    /// "Upsert" while the strategy line named three members that did not include it.</summary>
    [Fact]
    public void An_Example_Uses_A_Value_The_Option_Lists()
    {
        var help = _help.GetAdapterHelp("sqlite");
        var strategyLine = help.Split('\n').First(l => l.TrimStart().StartsWith("strategy: Append", StringComparison.Ordinal));

        Assert.Contains("strategy: \"Upsert\"", help);
        Assert.Contains("Upsert", strategyLine);
    }

    /// <summary>
    /// The catalogue listing names what each adapter does, not just what it is called. A model
    /// reading names alone has to open every component's help to find the one it needs, and a
    /// recorded session shows it running out of turns before reaching the one that unblocked it.
    /// </summary>
    [Fact]
    public void Every_Listed_Adapter_Says_What_It_Does()
    {
        var listed = _help.Adapters();

        Assert.Equal(_roles.Select(r => r.Adapter).Distinct(StringComparer.OrdinalIgnoreCase).Count(), listed.Count);
        foreach (var a in listed)
            Assert.False(string.IsNullOrWhiteSpace(a.Description), $"adapter '{a.Name}' is listed without a description");
    }

    /// <summary>An adapter's roles are part of discovery: asking for transformer help on a reader
    /// is a wrong turn the listing can prevent rather than answer afterwards.</summary>
    [Fact]
    public void The_Listing_Says_Which_Roles_An_Adapter_Supports()
    {
        var listed = _help.Adapters().ToDictionary(a => a.Name, StringComparer.OrdinalIgnoreCase);

        Assert.Equal("reader", listed["generate"].Roles);
        Assert.Equal("writer", listed["null"].Roles);
        Assert.Equal("reader, writer", listed["sqlite"].Roles);
    }

    /// <summary>
    /// The general help is served to the planner role, which is told 'execute-yaml-job' is
    /// unavailable and must not be looked for. Naming it here made the first call of a planning
    /// session contradict the role prompt that had just been read.
    /// </summary>
    [Fact]
    public void The_General_Help_Names_No_Execution_Tool()
        => Assert.DoesNotContain("execute-yaml-job", _help.GetGeneralHelp());

    /// <summary>The general help carries the same listing, so the two cannot drift.</summary>
    [Fact]
    public void The_General_Help_Carries_The_Descriptions_Too()
    {
        var help = _help.GetGeneralHelp();

        foreach (var a in _help.Adapters())
            Assert.Contains(a.Description, help);
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
