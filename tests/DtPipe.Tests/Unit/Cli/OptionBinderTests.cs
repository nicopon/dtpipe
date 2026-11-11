using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F8 — one canonical binder. Theory over 10+ option classes:
/// 1. The binder's inverse map covers every flag name produced by flag-def generation.
/// 2. CLI args and the equivalent YAML dictionary produce equal options objects.
/// 3. Unknown YAML keys warn (lenient) or throw (strict).
/// </summary>
[Collection("console-serial")]
public class OptionBinderTests
{
    /// <summary>Captures Console.Error while running <paramref name="action"/>. The class joins the console-serial collection: the redirect is process-wide, so a test writing to stderr in another collection would land in this capture.</summary>
    private static string CaptureStderr(Action action)
    {
        var original = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try { action(); }
        finally { Console.SetError(original); }
        return captured.ToString();
    }

    public static TheoryData<Type> OptionTypes => new()
    {
        typeof(DtPipe.Adapters.Csv.CsvReaderOptions),
        typeof(DtPipe.Adapters.Csv.CsvWriterOptions),
        typeof(DtPipe.Adapters.PostgreSQL.PostgreSqlReaderOptions),
        typeof(DtPipe.Adapters.PostgreSQL.PostgreSqlWriterOptions),
        typeof(DtPipe.Adapters.DuckDB.DuckDbWriterOptions),
        typeof(DtPipe.Transformers.Arrow.Fake.FakeOptions),
        typeof(DtPipe.Transformers.Arrow.Filter.FilterOptions),
        typeof(DtPipe.Transformers.Row.Compute.ComputeOptions),
        typeof(DtPipe.Transformers.Arrow.Null.NullOptions),
        typeof(DtPipe.Transformers.Arrow.Project.ProjectOptions),
        typeof(DtPipe.Transformers.Row.Window.WindowOptions),
    };

    private static Dictionary<string, PropertyInfo> FlagMap(Type t)
    {
        var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var metadata = (Activator.CreateInstance(t) as ICliOptionMetadata)?.PropertyToFlag;
        var map = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        foreach (var prop in props)
        {
            var canonical = FlagNameDeriver.DeriveCanonical(prop, t, metadata);
            map[canonical] = prop;
            foreach (var alias in prop.GetCustomAttributes<ComponentOptionAttribute>(true).SelectMany(a => a.Aliases ?? Array.Empty<string>()))
                map[alias] = prop;
        }
        return map;
    }

    private static IEnumerable<PropertyInfo> CliVisibleProps(Type t)
        => t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetCustomAttribute<ComponentOptionAttribute>() is not null
                        || p.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>() is not null);

    // ── 1. Binder inverse covers generator output ───────────────────────────

    [Theory]
    [MemberData(nameof(OptionTypes))]
    public void Binder_Inverse_Map_Covers_All_Generated_Flag_Names(Type optionsType)
    {
        var generated = CliOptionBuilder.GenerateFlagDefsForType(optionsType).ToList();
        Assert.NotEmpty(generated);

        var map = FlagMap(optionsType);
        foreach (var def in generated)
            Assert.True(map.ContainsKey(def.Name),
                $"Flag '{def.Name}' of {optionsType.Name} has no property mapping on the binder side.");
    }

    // ── 2. CLI args ≡ YAML dict ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(OptionTypes))]
    public void Yaml_And_Cli_Produce_Identical_Options(Type optionsType)
    {
        // Sample up to three bindable scalar properties.
        var samples = new List<(PropertyInfo Prop, string Value)>();
        foreach (var prop in CliVisibleProps(optionsType))
        {
            if (samples.Count >= 3) break;
            var underlying = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            if (underlying == typeof(string))
            {
                if (prop.SetMethod == null) continue;
                samples.Add((prop, "x-value"));
            }
            else if (underlying == typeof(int))
            {
                if (prop.SetMethod == null) continue;
                samples.Add((prop, "42"));
            }
        }

        if (samples.Count == 0) return; // nothing generically samplable for this type

        var registry = new FlagRegistry();
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(optionsType))
            registry.Register(def);

        var map = FlagMap(optionsType);

        // CLI form: canonical flag name + value tokens
        var cliArgs = new List<string>();
        foreach (var (prop, value) in samples)
        {
            var canonical = map.First(kv => kv.Value == prop).Key;
            cliArgs.Add(canonical);
            cliArgs.Add(value);
        }
        var cliInstance = Activator.CreateInstance(optionsType)!;
        OptionBinder.BindCli(cliInstance, cliArgs.ToArray(), registry);

        // YAML form: normalized property name keys
        var yaml = samples.ToDictionary(s => s.Prop.Name, s => (object?)s.Value);
        var yamlInstance = Activator.CreateInstance(optionsType)!;
        OptionBinder.BindYaml(yamlInstance, yaml);

        foreach (var (prop, value) in samples)
        {
            var cliVal = prop.GetValue(cliInstance)?.ToString();
            var yamlVal = prop.GetValue(yamlInstance)?.ToString();
            Assert.Equal(value, cliVal);
            Assert.Equal(value, yamlVal);
        }
    }

    // ── 3. Unknown YAML key handling ────────────────────────────────────────

    private sealed class ProbeOptions : DtPipe.Core.Options.IOptionSet
    {
        public static string Prefix => "probe";
        public static string DisplayName => "Probe";
        [ComponentOption("--known", Description = "known")]
        public string Known { get; set; } = "";
    }

    [Fact]
    public void Unknown_Yaml_Key_Warns_When_Lenient()
    {
        var instance = new ProbeOptions();
        var stderr = CaptureStderr(() =>
            OptionBinder.BindYaml(instance, new Dictionary<string, object?> { ["totally-unknown"] = "v" }));

        Assert.Equal("", instance.Known); // untouched
        Assert.Contains("Unrecognized option 'totally-unknown' for 'ProbeOptions'", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_Yaml_Key_Throws_When_Strict()
    {
        var instance = new ProbeOptions();
        var ex = Assert.Throws<InvalidOperationException>(() =>
            OptionBinder.BindYaml(instance, new Dictionary<string, object?> { ["totally-unknown"] = "v" }, strict: true));
        Assert.Contains("totally-unknown", ex.Message);
    }

    // ── 4. Required enforcement ─────────────────────────────────────────────

    private sealed class RequiredOptions : DtPipe.Core.Options.IOptionSet
    {
        public static string Prefix => "req";
        public static string DisplayName => "Required";

        [ComponentOption("--table", Required = true)]
        public string Table { get; set; } = "";
    }

    [Fact]
    public void Required_Violation_Warns_When_Lenient()
    {
        var instance = new RequiredOptions();
        var stderr = CaptureStderr(() => OptionBinder.EnforceRequired(instance, strict: false)); // must not throw

        Assert.Contains("Required option(s) missing on RequiredOptions: Table", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Required_Violation_Throws_When_Strict_And_Lists_Offender()
    {
        var instance = new RequiredOptions { Table = "" };
        var ex = Assert.Throws<InvalidOperationException>(() => OptionBinder.EnforceRequired(instance, strict: true));
        Assert.Contains("Table", ex.Message);

        // Filled option → no violation even in strict mode.
        OptionBinder.EnforceRequired(new RequiredOptions { Table = "events" }, strict: true);
    }
}
