using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Attributes;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;

namespace DtPipe.Cli.Mcp;

public class McpHelpService : IMcpHelpService
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;

    public McpHelpService(
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
    {
        _readerFactories = readerFactories;
        _transformerFactories = transformerFactories;
        _writerFactories = writerFactories;
    }

    public string GetGeneralHelp()
    {
        using var sw = new StringWriter();
        sw.WriteLine("dtpipe — Data streaming & anonymization engine");
        sw.WriteLine();
        sw.WriteLine("YAML JOB USAGE (RECOMMENDED FOR AGENTS):");
        sw.WriteLine("  To run pipelines, execute a YAML job configuration using the 'execute-yaml-job' tool.");
        sw.WriteLine("  This is highly structured and completely avoids command-line quoting or shell escaping issues.");
        sw.WriteLine();
        sw.WriteLine("YAML JOB STRUCTURE:");
        sw.WriteLine("  A YAML job configuration is defined by named branches (typically 'main' for simple pipelines):");
        sw.WriteLine("  main:");
        sw.WriteLine("    input: \"<adapter-prefix>:<connection-string-or-file>\"");
        sw.WriteLine("    output: \"<adapter-prefix>:<connection-string-or-file>\"");
        sw.WriteLine("    provider-options:           # Optional: adapter-specific settings");
        sw.WriteLine("      <adapter-name>-reader:");
        sw.WriteLine("        <option-name>: <option-value>");
        sw.WriteLine("      <adapter-name>-writer:");
        sw.WriteLine("        <option-name>: <option-value>");
        sw.WriteLine("    transformers:               # Optional: list of transformers");
        sw.WriteLine("      - type: <transformer-name>");
        sw.WriteLine("        mappings:               # Column-level transformations");
        sw.WriteLine("          <column-name>: <expression_or_faker>");
        sw.WriteLine("        options:                # Transformer-level options");
        sw.WriteLine("          <option-name>: <option-value>");
        sw.WriteLine();
        sw.WriteLine("CONNECTION STRINGS:");
        sw.WriteLine("  - Connection strings follow the format '<provider-prefix>:<path-or-connection-string>'.");
        sw.WriteLine("  - File providers read/write file paths or '-' for STDIN/STDOUT.");
        sw.WriteLine("  - Database providers use ADO.NET connection strings (semicolon-separated Key=Value; pairs).");
        sw.WriteLine("  - An adapter's example shows the minimum keys required, not the full set. ADO.NET fixes the");
        sw.WriteLine("    Key=Value form but not the vocabulary: the complete option list belongs to that provider's");
        sw.WriteLine("    .NET driver, named in its adapter help.");
        sw.WriteLine("  - Call 'get-adapter-help <adapter-name>' to inspect the exact connection string syntax and options for any adapter.");
        sw.WriteLine();
        sw.WriteLine("DAG TOPOLOGIES & ROUTING IN YAML:");
        sw.WriteLine("  dtpipe can execute multi-branch pipelines forming a Directed Acyclic Graph (DAG) by defining multiple named branches.");
        sw.WriteLine("  Note: Branch aliases are used as table names in SQL queries. Prefer simple alphanumeric alias names without hyphens (e.g. 'branch1', 'sales') or enclose hyphenated names in double quotes in SQL (e.g. \"sales-branch\").");
        sw.WriteLine("  branch1:");
        sw.WriteLine("    input: \"<adapter1>:<source1>\"");
        sw.WriteLine("  branch2:");
        sw.WriteLine("    input: \"<adapter2>:<source2>\"");
        sw.WriteLine("  main:");
        sw.WriteLine("    from: \"branch1\"            # Streaming alias source");
        sw.WriteLine("    ref: [ \"branch2\" ]         # Preloaded lookup references");
        sw.WriteLine("    provider-options:");
        sw.WriteLine("      sql:");
        sw.WriteLine("        query: \"SELECT * FROM branch1 JOIN branch2 ON branch1.id = branch2.id\"");
        sw.WriteLine("    output: \"<adapter3>:<target>\"");
        sw.WriteLine();
        sw.WriteLine("VALUE RESOLUTION & INLINE INTERPOLATION:");
        sw.WriteLine("  Values are resolved sequentially prior to execution:");
        sw.WriteLine("  - @/path/to/file             Loads full content from file.");
        sw.WriteLine("  - keyring://<alias>          Loads secret value from OS Keyring.");
        sw.WriteLine("  - ${{ENV_VAR}}               Substitutes environment variable.");
        sw.WriteLine("  - ${{keyring://<alias>}}     Substitutes inline keyring secret.");
        sw.WriteLine("  - ${{cursor://path|default}}  Substitutes incremental cursor value from a state file.");
        sw.WriteLine();
        sw.WriteLine("INCREMENTAL SYNC:");
        sw.WriteLine("  Define these keys directly in the branch root:");
        sw.WriteLine("  - cursor: <column_name>");
        sw.WriteLine("  - state: <path_to_state_file>");
        sw.WriteLine();

        sw.WriteLine("ADAPTERS:");
        foreach (var a in Adapters())
            sw.WriteLine($"  {a.Name.PadRight(14)} ({a.Roles}) {a.Description}");
        sw.WriteLine("  To see connection string rules, YAML schema options, and examples for a specific adapter, call 'get-adapter-help <adapter-name>'.");
        sw.WriteLine();

        sw.WriteLine("TRANSFORMERS:");
        foreach (var t in Transformers())
            sw.WriteLine($"  {t.Name.PadRight(14)} {t.Description}");
        sw.WriteLine("  To see YAML schema mappings, options, and examples for a specific transformer, call 'get-transformer-help <transformer-name>'.");
        sw.WriteLine();

        return sw.ToString();
    }

    /// <summary>
    /// The adapter catalogue as a caller discovering it sees it. The description is the
    /// component's own <see cref="DescriptionAttribute"/> — the same one
    /// <see cref="GetAdapterHelp"/> prints — so a listing cannot drift from the detailed help.
    /// The reader's is preferred when both roles carry one; the roles say which are available.
    /// </summary>
    public IReadOnlyList<ComponentSummary> Adapters()
    {
        var readers = _readerFactories.ToDictionary(f => f.ComponentName.ToLowerInvariant(), f => f.OptionsType, StringComparer.OrdinalIgnoreCase);
        var writers = _writerFactories.ToDictionary(f => f.ComponentName.ToLowerInvariant(), f => f.OptionsType, StringComparer.OrdinalIgnoreCase);

        return readers.Keys.Union(writers.Keys, StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(name =>
            {
                var hasReader = readers.TryGetValue(name, out var readerType);
                var hasWriter = writers.TryGetValue(name, out var writerType);
                var roles = hasReader && hasWriter ? "reader, writer" : hasReader ? "reader" : "writer";
                return new ComponentSummary(name, roles, Describe(hasReader ? readerType : writerType));
            })
            .ToList();
    }

    /// <inheritdoc />
    public IReadOnlyList<ComponentSummary> Transformers()
        => _transformerFactories
            .Select(f => new ComponentSummary(f.ComponentName.ToLowerInvariant(), "transformer", Describe(f.OptionsType)))
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .ToList();

    private static string Describe(Type? optionsType)
        => optionsType?.GetCustomAttribute<DescriptionAttribute>()?.Description ?? string.Empty;

    public string GetAdapterHelp(string adapterName)
    {
        if (string.IsNullOrWhiteSpace(adapterName))
            return JsonSerializer.Serialize(new { error = "Adapter name cannot be empty." });

        var normalized = adapterName.Trim().ToLowerInvariant();
        var readers = _readerFactories.Where(f => f.ComponentName.Equals(normalized, StringComparison.OrdinalIgnoreCase)).ToList();
        var writers = _writerFactories.Where(f => f.ComponentName.Equals(normalized, StringComparison.OrdinalIgnoreCase)).ToList();

        if (readers.Count == 0 && writers.Count == 0)
            return Unknown("adapter", adapterName, AdapterNames(), TransformerNames(), "transformer");

        using var sw = new StringWriter();
        sw.WriteLine($"ADAPTER: {normalized}");
        sw.WriteLine(new string('=', normalized.Length + 9));
        sw.WriteLine();

        // Reader and writer each carry their own [Description] and [ComponentHelp]. Both are
        // emitted: taking only the first left every writer's usage notes unreachable, which for a
        // database adapter is where the upsert key requirement and bulk-load prerequisites live.
        var roleTypes = readers.Select(r => (Role: "Reader", r.OptionsType))
            .Concat(writers.Select(w => (Role: "Writer", w.OptionsType)))
            .GroupBy(x => x.OptionsType)
            .Select(g => (Roles: string.Join(" / ", g.Select(x => x.Role).Distinct()), OptionsType: g.Key))
            .ToList();

        var described = false;
        foreach (var (roles, type) in roleTypes)
        {
            var descAttr = type.GetCustomAttribute<DescriptionAttribute>();
            if (descAttr == null) continue;
            sw.WriteLine(roleTypes.Count > 1 ? $"{roles}: {descAttr.Description}" : descAttr.Description);
            described = true;
        }
        if (described) sw.WriteLine();

        sw.WriteLine("YAML Provider Options Configuration:");
        sw.WriteLine($"  Place these under 'provider-options' -> '{normalized}' (or specific role suffix '{normalized}-reader' / '{normalized}-writer'):");

        if (readers.Count > 0)
        {
            sw.WriteLine("  Role: Reader (Data Source)");
            foreach (var r in readers)
            {
                FormatOptionProperties(sw, r.OptionsType, "    ");
            }
            sw.WriteLine();
        }

        if (writers.Count > 0)
        {
            sw.WriteLine("  Role: Writer (Data Destination)");
            foreach (var w in writers)
            {
                FormatOptionProperties(sw, w.OptionsType, "    ");
            }
            sw.WriteLine();
        }

        foreach (var (roles, type) in roleTypes)
        {
            FormatComponentHelp(sw, type, roleTypes.Count > 1 ? roles : null);
        }

        return sw.ToString();
    }

    /// <summary>
    /// The answer to a name that is not there. It names what is, and — when the name exists as
    /// another kind of component — says so.
    ///
    /// <para>
    /// Both matter, and a recorded session shows why. <c>list-providers</c> returns readers,
    /// transformers and writers in one payload, so a caller reading it takes a name away without
    /// its role; one asked for transformer help on <c>generate</c>, which is a reader, was told
    /// only that it did not exist, and spent three more turns rediscovering where it had seen the
    /// name. An error that closes the door without pointing at one costs more calls than it saves.
    /// </para>
    /// </summary>
    private static string Unknown(string kind, string requested,
        IReadOnlyList<string> available, IReadOnlyList<string> elsewhere, string elsewhereKind)
    {
        var normalized = requested.Trim().ToLowerInvariant();
        var otherRole = elsewhere.Any(n => n.Equals(normalized, StringComparison.OrdinalIgnoreCase))
            ? $"'{normalized}' is a {elsewhereKind}, not a {kind}."
            : null;

        return JsonSerializer.Serialize(new
        {
            error = $"Unknown {kind} '{requested}'.",
            hint = otherRole,
            available,
        });
    }

    private IReadOnlyList<string> AdapterNames() =>
        _readerFactories.Select(f => f.ComponentName)
            .Concat(_writerFactories.Select(f => f.ComponentName))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

    private IReadOnlyList<string> TransformerNames() =>
        _transformerFactories.Select(f => f.ComponentName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

    public string GetTransformerHelp(string transformerName)
    {
        if (string.IsNullOrWhiteSpace(transformerName))
            return JsonSerializer.Serialize(new { error = "Transformer name cannot be empty." });

        var normalized = transformerName.Trim().ToLowerInvariant();
        var factory = _transformerFactories.FirstOrDefault(f => f.ComponentName.Equals(normalized, StringComparison.OrdinalIgnoreCase));
        if (factory == null)
            return Unknown("transformer", transformerName, TransformerNames(), AdapterNames(), "adapter");

        using var sw = new StringWriter();
        sw.WriteLine($"TRANSFORMER: {normalized}");
        sw.WriteLine(new string('=', normalized.Length + 13));
        sw.WriteLine();

        var descAttr = factory.OptionsType.GetCustomAttribute<DescriptionAttribute>();
        if (descAttr != null)
        {
            sw.WriteLine(descAttr.Description);
            sw.WriteLine();
        }

        var skipNames = new[] { normalized, "filters", "mask", "fake" };
        var properties = factory.OptionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .ToList();

        if (properties.Count > 0)
        {
            sw.WriteLine("YAML Options Configuration:");
            sw.WriteLine("  Place these options under the 'options' block of the transformer:");
            FormatOptionProperties(sw, factory.OptionsType, "  ", skipNames);
            sw.WriteLine();
        }

        FormatComponentHelp(sw, factory.OptionsType);

        if (normalized == "fake")
        {
            sw.WriteLine(GetAnonymizationHelp());
        }

        return sw.ToString();
    }

    public string GetAnonymizationHelp()
    {
        using var sw = new StringWriter();
        sw.WriteLine("ANONYMIZATION VIA FAKERS (YAML SCHEMA):");
        sw.WriteLine("  dtpipe uses the 'Bogus' library for generating fake data.");
        sw.WriteLine("  Specify column-level fakers in the 'mappings' section of the 'fake' transformer.");
        sw.WriteLine("  Syntax (under mappings):");
        sw.WriteLine("    <column_name>: <dataset>.<method>");
        sw.WriteLine("  Example YAML:");
        sw.WriteLine("    transformers:");
        sw.WriteLine("      - type: fake");
        sw.WriteLine("        mappings:");
        sw.WriteLine("          Email: internet.email");
        sw.WriteLine("          Name: name.fullName");
        sw.WriteLine();
        sw.WriteLine("AVAILABLE DATASETS & METHODS (DYNAMICALLY RESOLVED):");

        var fakerRegistry = new DtPipe.Transformers.Arrow.Fake.FakerRegistry();
        foreach (var group in fakerRegistry.ListAll())
        {
            sw.WriteLine($"  - {group.Dataset.ToLowerInvariant()}");
            foreach (var method in group.Methods)
            {
                var paddedMethod = method.Method.PadRight(25);
                var desc = string.IsNullOrEmpty(method.Description) ? "" : $" {method.Description}";
                sw.WriteLine($"    - {paddedMethod}{desc}");
            }
        }

        sw.WriteLine();
        sw.WriteLine("ANONYMIZATION OPTIONS (DYNAMICALLY RESOLVED FROM COMPONENT OPTIONS):");
        FormatOptionProperties(sw, typeof(DtPipe.Transformers.Arrow.Fake.FakeOptions), "  ", new[] { "fake" });
        return sw.ToString();
    }

    private static void FormatOptionProperties(TextWriter writer, Type optionsType, string indent, IEnumerable<string>? skipNames = null)
    {
        var skipSet = skipNames != null ? new HashSet<string>(skipNames, StringComparer.OrdinalIgnoreCase) : null;
        var properties = optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .ToList();

        foreach (var prop in properties)
        {
            var kebabName = prop.Name.ToKebabCase();
            if (skipSet != null && skipSet.Contains(kebabName))
                continue;

            var cliOptionAttr = prop.GetCustomAttribute<ComponentOptionAttribute>();
            var descriptionAttr = prop.GetCustomAttribute<DescriptionAttribute>();
            var desc = CliOptionBuilder.ResolveDescription(cliOptionAttr, descriptionAttr);

            writer.WriteLine($"{indent}{kebabName}: <value>");
            if (!string.IsNullOrEmpty(desc))
            {
                writer.WriteLine($"{indent}  # {desc}");
            }
        }
    }

    /// <param name="role">Section label when one adapter exposes more than one role, else null.</param>
    private static void FormatComponentHelp(TextWriter writer, Type optionsType, string? role = null)
    {
        var helpAttr = optionsType.GetCustomAttribute<ComponentHelpAttribute>();
        if (helpAttr == null) return;

        var suffix = role is null ? string.Empty : $" ({role})";

        if (!string.IsNullOrWhiteSpace(helpAttr.UsageNotes))
        {
            writer.WriteLine($"YAML Usage & Notes{suffix}:");
            writer.WriteLine($"  {helpAttr.UsageNotes}");
            writer.WriteLine();
        }

        if (helpAttr.Examples != null && helpAttr.Examples.Length > 0)
        {
            writer.WriteLine($"YAML Example Configuration{suffix}:");
            foreach (var ex in helpAttr.Examples)
            {
                writer.WriteLine(ex);
            }
            writer.WriteLine();
        }
    }
}
