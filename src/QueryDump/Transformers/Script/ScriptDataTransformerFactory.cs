using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Script;

public class ScriptDataTransformerFactory : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry;

    public ScriptDataTransformerFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<ScriptDataTransformer>();
    }

    public string Category => "Transformer Options";
    public string TransformerType => ScriptOptions.Prefix;

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
        var scriptOptions = _registry.Get<ScriptOptions>();
        if (scriptOptions.Mappings.Count == 0)
        {
            return null;
        }
        return new ScriptDataTransformer(scriptOptions);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
    {
        // Simple manual parsing or reuse builder if needed, but here simple enough
        var mappings = new List<string>();
        bool skipNull = false;
        foreach (var (option, value) in configuration)
        {
            if (option == "script" || option == "--script") 
            {
                mappings.Add(value);
            }
            else if (option == "script-skip-null" || option == "--script-skip-null")
            {
                if (bool.TryParse(value, out var b)) skipNull = b;
            }
        }
        
        return new ScriptDataTransformer(new ScriptOptions { Mappings = mappings, SkipNull = skipNull });
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Mappings == null || config.Mappings.Count == 0) return null;

        var mappings = config.Mappings.Select(kvp => $"{kvp.Key}:{kvp.Value}").ToList();
        
        bool skipNull = false;
        if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
        {
             bool.TryParse(snStr, out skipNull);
        }

        return new ScriptDataTransformer(new ScriptOptions { Mappings = mappings, SkipNull = skipNull });
    }

    public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
    {
        return Task.FromResult<int?>(null);
    }
}
