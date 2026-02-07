using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Script;

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
        
        if (scriptOptions.Script == null || !scriptOptions.Script.Any())
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
            else if (option == "script-skip-null" || option == "--script-skip-null")
            {
                if (bool.TryParse(value, out var b)) skipNull = b;
            }
        }
        
        return new ScriptDataTransformer(new ScriptOptions { Script = mappings, SkipNull = skipNull });
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Script == null || !config.Script.Any()) return null;

        var mappings = config.Script.Select(kvp => $"{kvp.Key}:{ResolveScriptContent(kvp.Value)}").ToList();
        
        bool skipNull = false;
        if (config.Options != null && config.Options.TryGetValue("skip-null", out var snStr))
        {
             bool.TryParse(snStr, out skipNull);
        }

        return new ScriptDataTransformer(new ScriptOptions { Script = mappings, SkipNull = skipNull });
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
