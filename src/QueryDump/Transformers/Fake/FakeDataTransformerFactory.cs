using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory : IDataTransformerFactory
{
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
    private readonly OptionsRegistry _registry;
    private IEnumerable<Option>? _cliOptions;
    private Dictionary<string, string>? _aliasToProperty;

    public FakeDataTransformerFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<FakeDataTransformer>();
    }

    public string Category => "Transformer Options";
    
    public string TransformerType => FakeOptions.Prefix; // "fake"

    public IEnumerable<Option> GetCliOptions()
    {
        if (_cliOptions != null) return _cliOptions;

        // Manual option for listing fakers (not bound to POCO yet)
        var list = new List<Option> 
        {
            new Option<bool>("--fake-list")
            {
                Description = "List all available fake data generators and exit"
            }
        };

        foreach (var type in GetSupportedOptionTypes())
        {
            var (options, aliasMap) = CliOptionBuilder.GenerateOptionsWithMetadataForType(type);
            list.AddRange(options);
            _aliasToProperty = aliasMap; // Store for CreateFromConfiguration
        }

        return _cliOptions = list;
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
        var fakeOptions = _registry.Get<FakeOptions>();

        if (fakeOptions.Mappings.Count == 0)
        {
            return null;
        }

        return new FakeDataTransformer(fakeOptions);
    }

    public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
    {
        // Ensure alias map is initialized
        if (_aliasToProperty == null) GetCliOptions();
        
        // Defaults from registry
        var globalOptions = _registry.Get<FakeOptions>();
        
        var mappings = new List<string>();
        var locale = globalOptions.Locale;
        var seedColumn = globalOptions.SeedColumn;
        var deterministic = globalOptions.Deterministic;
        var seed = globalOptions.Seed;

        foreach (var (option, value) in configuration)
        {
            // Lookup property name from option alias - NO HARDCODED STRINGS
            if (_aliasToProperty!.TryGetValue(option, out var propertyName))
            {
                switch (propertyName)
                {
                    case nameof(FakeOptions.Mappings):
                        mappings.Add(value);
                        break;
                    case nameof(FakeOptions.Locale):
                        locale = value;
                        break;
                    case nameof(FakeOptions.SeedColumn):
                        seedColumn = value;
                        break;
                    case nameof(FakeOptions.Seed):
                        if (int.TryParse(value, out var s)) seed = s;
                        break;
                    case nameof(FakeOptions.Deterministic):
                        if (bool.TryParse(value, out var d)) deterministic = d;
                        break;
                }
            }
            // Unknown options are ignored (could be from --fake-list or other)
        }
        
        var options = new FakeOptions
        {
            Mappings = mappings,
            Locale = locale,
            Seed = seed,
            SeedColumn = seedColumn,
            Deterministic = deterministic
        };
        
        return new FakeDataTransformer(options);
    }

    public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
    {
        if (config.Mappings == null || config.Mappings.Count == 0)
            return null;

        // Convert YAML dict mappings to list format: "COLUMN:faker.method"
        var mappings = config.Mappings.Select(kvp => $"{kvp.Key}:{kvp.Value}").ToList();
        
        // Parse options from YAML
        var locale = "en";
        string? seedColumn = null;
        int? seed = null;
        var deterministic = false;

        if (config.Options != null)
        {
            if (config.Options.TryGetValue("locale", out var loc)) locale = loc;
            if (config.Options.TryGetValue("seed-column", out var sc)) seedColumn = sc;
            if (config.Options.TryGetValue("seed", out var seedStr) && int.TryParse(seedStr, out var s)) seed = s;
            if (config.Options.TryGetValue("deterministic", out var detStr) && bool.TryParse(detStr, out var d)) deterministic = d;
        }

        var options = new FakeOptions
        {
            Mappings = mappings,
            Locale = locale,
            Seed = seed,
            SeedColumn = seedColumn,
            Deterministic = deterministic
        };
        
        return new FakeDataTransformer(options);
    }
    
    public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
    {
        // Check for --fake-list flag
        if (parseResult.Tokens.Any(t => t.Value == "--fake-list"))
        {
             // Handle flag if present
             var isFakeList = parseResult.GetValue<bool>("--fake-list");
             if (isFakeList)
             {
                 PrintFakerList();
                 return Task.FromResult<int?>(0);
             }
        }
        return Task.FromResult<int?>(null);
    }
    
    private static void PrintFakerList()
    {
        var registry = new FakerRegistry();
        Console.WriteLine("Available fakers (use format: COLUMN:dataset.method)");
        Console.WriteLine();
        foreach (var (dataset, methods) in registry.ListAll())
        {
            Console.WriteLine($"{char.ToUpper(dataset[0])}{dataset[1..]}:");
            foreach (var (method, description) in methods)
            {
                Console.WriteLine($"  {$"{dataset}.{method}".ToLowerInvariant(),-30} {description}");
            }
            Console.WriteLine();
        }
    }
}
