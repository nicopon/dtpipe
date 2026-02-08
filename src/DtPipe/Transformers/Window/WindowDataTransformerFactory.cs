using DtPipe.Cli;
using System.CommandLine;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Services;

namespace DtPipe.Transformers.Window;

public class WindowDataTransformerFactory : IDataTransformerFactory
{
    private readonly OptionsRegistry _registry;
    private readonly IJsEngineProvider _jsEngineProvider;

    public WindowDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
    {
        _registry = registry;
        _jsEngineProvider = jsEngineProvider;
    }

    public string Category => "Transformer Options";
    public string TransformerType => "window";

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

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<WindowDataTransformer>();
    }

    public IDataTransformer? Create(Configuration.DumpOptions options)
    {
        var windowOptions = _registry.Get<WindowOptions>();
        
        // Only create if any window option is set
        if (windowOptions.Count.HasValue || !string.IsNullOrEmpty(windowOptions.Key) || !string.IsNullOrEmpty(windowOptions.Script))
        {
            return new WindowDataTransformer(windowOptions, _jsEngineProvider);
        }

        return null;
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
    {
        // This is used for pipeline string configuration e.g. "window:count=100;script='...'"
        // But current architecture uses options registry for typed options?
        // Actually, CreateFromConfiguration is likely used by PipelineBuilder when parsing --pipeline "transform1:args...".
        // We need to parse 'Value' into WindowOptions.
        
        var options = new WindowOptions();
        
        foreach (var (key, val) in configuration)
        {
            if (key.Equals("count", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(val, out int c)) options.Count = c;
            }
            else if (key.Equals("key", StringComparison.OrdinalIgnoreCase))
            {
                options.Key = val;
            }
            else if (key.Equals("script", StringComparison.OrdinalIgnoreCase))
            {
                options.Script = val;
            }
        }
        
        return new WindowDataTransformer(options, _jsEngineProvider);
    }

    public IDataTransformer? CreateFromYamlConfig(Configuration.TransformerConfig config)
    {
        // For YAML, we map properties from dictionary
        var options = new WindowOptions();
        
        if (config.Options != null && config.Options.TryGetValue("count", out var countVal) && int.TryParse(countVal, out var c))
        {
            options.Count = c;
        }
        
        if (config.Options != null && config.Options.TryGetValue("key", out var keyVal))
        {
            options.Key = keyVal;
        }
        
        // YAML Script might be the main property?
        // Or specific 'script' option.
        if (config.Script != null)
        {
             // Join lines? Or take first? Window script should be single string.
             options.Script = string.Join("\n", config.Script);
        }
        else if (config.Options != null && config.Options.TryGetValue("script", out var scriptVal))
        {
             options.Script = scriptVal;
        }
        
        return new WindowDataTransformer(options, _jsEngineProvider);
    }
}
