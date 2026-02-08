using System.CommandLine;
using DtPipe.Cli;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Services;

namespace DtPipe.Transformers.Script;

public class ComputeDataTransformerFactory : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry;
    private readonly IJsEngineProvider _jsEngineProvider;

    public ComputeDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
    {
        _registry = registry;
        _jsEngineProvider = jsEngineProvider;
    }

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<ComputeDataTransformer>();
    }

    public string Category => "Transformer Options";
    public string TransformerType => ComputeOptions.Prefix;

    public IEnumerable<Option> GetCliOptions()
    {
        var list = new List<Option>();
        foreach (var type in GetSupportedOptionTypes())
        {
            var (options, _) = CliOptionBuilder.GenerateOptionsWithMetadataForType(type);
            list.AddRange(options);
        }
        return list;
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var allOptions = GetCliOptions();
        foreach (var type in GetSupportedOptionTypes())
        {
            var boundOptions = CliOptionBuilder.BindForType(type, parseResult, allOptions);
            registry.RegisterByType(type, boundOptions);
        }
    }

    public IDataTransformer? Create(DumpOptions options)
    {
        var computeOptions = _registry.Get<ComputeOptions>();
        
        if (computeOptions.Compute == null || !computeOptions.Compute.Any())
        {
            return null;
        }

        return new ComputeDataTransformer(computeOptions, _jsEngineProvider);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
    {
        // Simple manual parsing or reuse builder if needed, but here simple enough
        var mappings = new List<string>();
        bool skipNull = false;
        foreach (var (option, value) in configuration)
        {
            if (option == "compute" || option == "--compute" || option == "script" || option == "--script") 
            {
                var parts = value.Split(':', 2);
                if (parts.Length == 2)
                {
                    mappings.Add($"{parts[0]}:{ResolveScriptContent(parts[1])}");
                }
                else
                {
                     mappings.Add(value);
                }
            }
            else if (option == "compute-skip-null" || option == "--compute-skip-null" || option == "script-skip-null" || option == "--script-skip-null")
            {
                if (bool.TryParse(value, out var b)) skipNull = b;
            }
        }
        
        return new ComputeDataTransformer(new ComputeOptions { Compute = mappings, SkipNull = skipNull }, _jsEngineProvider);
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        // Support both specific 'compute' config or legacy 'script' if mapped
        // But usually YAML config is a dictionary.
        // If config.Script is populated (from legacy mapping), use it.
        // We probably need to update TransformerConfig to support Compute?
        // Or just map it here?
        
        var mappings = new List<string>();
        
        if (config.Script != null && config.Script.Any())
        {
            mappings.AddRange(config.Script.Select(kvp => $"{kvp.Key}:{ResolveScriptContent(kvp.Value)}"));
        }
        
        // For now, assume Script property covers it or we look at Options.
        // If we want to support 'compute: { col: val }' in YAML, we will need to update TransformerConfig.
        // For this refactor, let's rely on Script property being the carrier.
        
        if (!mappings.Any()) return null;

        bool skipNull = false;
        if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
        {
             bool.TryParse(snStr, out skipNull);
        }

        return new ComputeDataTransformer(new ComputeOptions { Compute = mappings, SkipNull = skipNull }, _jsEngineProvider);
    }

    public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
    {
        return Task.FromResult<int?>(null);
    }
    private static string ResolveScriptContent(string script)
    {
        if (string.IsNullOrWhiteSpace(script)) return script;

        // Explicit @ syntax
        if (script.StartsWith("@"))
        {
             var path = script.Substring(1);
             if (File.Exists(path))
             {
                 return File.ReadAllText(path);
             }
             // Fallback: return as-is if file not found
             return script;
        }

        // Implicit syntax
        // Only load if it looks like a file path (not a short script like "return 1;")
        // But to be consistent with --query, we just check existence.
        // A script "return 1;" is unlikely to match a filename unless someone is very evil.
        if (File.Exists(script))
        {
            return File.ReadAllText(script);
        }

        return script;
    }
}
