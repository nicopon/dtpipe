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

public class FakeDataTransformerFactory(OptionsRegistry registry) : IFakeDataTransformerFactory
{
    private readonly OptionsRegistry _registry = registry;

    public static IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<FakeDataTransformer>();
    }

    public string Category => "Transformer Options";

    private IEnumerable<Option>? _cliOptions;

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
            list.AddRange(CliOptionBuilder.GenerateOptionsForType(type));
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

    public IDataTransformer CreateFromConfiguration(IEnumerable<string> values)
    {
        // Retrieve global options (like Locale, Seed) from registry as they apply to all fake instances
        var globalOptions = _registry.Get<FakeOptions>();
        
        var options = new FakeOptions
        {
            Mappings = values.ToList(),
            Locale = globalOptions.Locale,
            Seed = globalOptions.Seed,
            SeedColumn = globalOptions.SeedColumn,
            Deterministic = globalOptions.Deterministic
        };
        
        return new FakeDataTransformer(options);
    }
    
    public Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
    {
        // Check for --fake-list flag
        if (parseResult.Tokens.Any(t => t.Value == "--fake-list"))
        {
             // Check value if needed, though presence is usually enough for a bool flag if defined properly
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
