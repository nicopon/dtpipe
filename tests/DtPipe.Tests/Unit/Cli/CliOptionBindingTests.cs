using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Every option the catalogue publishes must actually arrive, and arrive identically through
/// both surfaces. Parameterised over the catalogue on the model of <c>RemoteUriClaimTests</c>:
/// a provider added is covered without editing this file.
/// </summary>
/// <remarks>
/// The binder's CLI half assigned nothing for a type it did not list, in silence:
/// <c>--row-count 1000</c> on a <c>long</c> wrote 100 rows and exited 0 while the same option in
/// YAML wrote 1000. The sampler below refuses a type it cannot represent rather than skipping it,
/// so an option type nobody has taught either side about fails here instead of in production.
/// </remarks>
public class CliOptionBindingTests
{
    public static TheoryData<string, Type, string> CatalogueOptions()
    {
        var data = new TheoryData<string, Type, string>();
        foreach (var (component, optionsType) in CatalogueOptionTypes())
            foreach (var prop in BindableProperties(optionsType))
                data.Add(component, optionsType, prop.Name);
        return data;
    }

    // ── 1. A CLI flag reaches the property it names ─────────────────────────

    [Theory]
    [MemberData(nameof(CatalogueOptions))]
    public void A_Provider_Option_Passed_On_The_Command_Line_Reaches_Its_Property(
        string component, Type optionsType, string propertyName)
    {
        var prop = optionsType.GetProperty(propertyName)!;
        Assert.True(prop.SetMethod is not null,
            $"'{component}' publishes the flag for {optionsType.Name}.{propertyName}, which has no setter, so nothing can bind to it.");

        var (tokens, expected) = Sample(component, prop);

        var registry = new FlagRegistry();
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(optionsType))
            registry.Register(def);

        var instance = Activator.CreateInstance(optionsType)!;
        OptionBinder.BindCli(instance, tokens, registry, component);

        AssertBound(prop.GetValue(instance), expected,
            $"'{tokens[0]}' was recognized for '{component}' and left {optionsType.Name}.{propertyName} untouched.");
    }

    // ── 2. The same option through YAML gives the same result ───────────────

    [Theory]
    [MemberData(nameof(CatalogueOptions))]
    public void The_Command_Line_And_The_Job_File_Agree_On_Every_Option(
        string component, Type optionsType, string propertyName)
    {
        var prop = optionsType.GetProperty(propertyName)!;
        if (prop.SetMethod is null) return; // reported by the test above

        var (tokens, _) = Sample(component, prop);

        var registry = new FlagRegistry();
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(optionsType))
            registry.Register(def);

        var fromCli = Activator.CreateInstance(optionsType)!;
        OptionBinder.BindCli(fromCli, tokens, registry, component);

        // A job file keys provider options by property name, never by flag.
        var yamlValue = tokens.Length > 1 ? tokens[1] : "true";
        var fromYaml = Activator.CreateInstance(optionsType)!;
        OptionBinder.BindYaml(fromYaml, new Dictionary<string, object?> { [propertyName] = yamlValue });

        AssertBound(prop.GetValue(fromCli), prop.GetValue(fromYaml),
            $"{optionsType.Name}.{propertyName} binds differently from the command line than from a job file.");
    }

    // ── Catalogue + sampling ────────────────────────────────────────────────

    private static IEnumerable<(string Component, Type OptionsType)> CatalogueOptionTypes()
    {
        var catalog = ComponentCatalog.Discover(
            typeof(DtPipe.Program).Assembly,
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
            typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

        var seen = new HashSet<(string, Type)>();
        foreach (var entry in catalog.Readers.Concat(catalog.Writers))
        {
            var factory = (IDataFactory)Activator.CreateInstance(entry.ImplementationType)!;
            if (seen.Add((entry.ComponentName, factory.OptionsType)))
                yield return (entry.ComponentName, factory.OptionsType);
        }
    }

    /// <summary>The properties the flag generator publishes, resolved back through the binder's own map.</summary>
    private static IEnumerable<PropertyInfo> BindableProperties(Type optionsType)
    {
        var names = CliOptionBuilder.GenerateFlagDefsForType(optionsType)
            .Select(d => d.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var metadata = (Activator.CreateInstance(optionsType) as DtPipe.Core.Options.ICliOptionMetadata)?.PropertyToFlag;

        foreach (var prop in optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            if (names.Contains(FlagNameDeriver.DeriveCanonical(prop, optionsType, metadata)))
                yield return prop;
    }

    /// <summary>
    /// The tokens that set <paramref name="prop"/>, and what it must hold afterwards. A value the
    /// property does not already carry, so an assignment that never happened cannot pass.
    /// </summary>
    private static (string[] Tokens, object? Expected) Sample(string component, PropertyInfo prop)
    {
        var flag = FlagNameDeriver.DeriveCanonical(prop, prop.DeclaringType!, null);
        var type = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

        string[] With(string value) => new[] { flag, value };

        if (type == typeof(bool)) return (new[] { flag }, true);
        if (type == typeof(string)) return (With("dtpipe-sample"), "dtpipe-sample");
        if (type == typeof(char)) return (With("|"), '|');
        if (type == typeof(Guid)) return (With("6f9619ff-8b86-d011-b42d-00c04fc964ff"), Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff"));
        if (type == typeof(TimeSpan)) return (With("00:00:07"), TimeSpan.FromSeconds(7));
        if (type == typeof(Dictionary<string, string>))
            return (With("a:1,b:2"), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["a"] = "1", ["b"] = "2" });

        if (type.IsEnum)
        {
            var member = Enum.GetValues(type).Cast<object>().Last();
            return (With(member.ToString()!), member);
        }

        if (type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type))
        {
            var element = type.IsArray ? type.GetElementType() : type.GetGenericArguments().FirstOrDefault();
            if (element == typeof(string))
                return (With("alpha,beta"), new[] { "alpha", "beta" });
        }

        if (type == typeof(double) || type == typeof(float) || type == typeof(decimal))
            return (With("0.5"), Convert.ChangeType(0.5, type));

        if (type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)
            || type == typeof(uint) || type == typeof(ulong) || type == typeof(sbyte) || type == typeof(ushort))
            return (With("7"), Convert.ChangeType(7, type));

        throw new Xunit.Sdk.XunitException(
            $"'{component}' publishes {prop.DeclaringType!.Name}.{prop.Name} of type {type.Name}, which this test "
            + "does not know how to write on a command line. Add it here and to OptionBinder's conversion, or the "
            + "flag binds nothing and nobody is told.");
    }

    private static void AssertBound(object? actual, object? expected, string because)
    {
        if (expected is IDictionary expectedMap && actual is IDictionary actualMap)
        {
            Assert.Equal(expectedMap.Count, actualMap.Count);
            foreach (DictionaryEntry entry in expectedMap)
                Assert.Equal(entry.Value, actualMap[entry.Key]);
            return;
        }

        if (expected is IEnumerable expectedSeq && expected is not string
            && actual is IEnumerable actualSeq && actual is not string)
        {
            Assert.Equal(expectedSeq.Cast<object?>(), actualSeq.Cast<object?>());
            return;
        }

        Assert.True(Equals(expected, actual), $"{because} Expected '{expected}', got '{actual}'.");
    }
}
