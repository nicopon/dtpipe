using System.CommandLine;
using QueryDump.Core.Options;

namespace QueryDump.Core;

/// <summary>
/// Base implementation for CLI contributors (readers, writers, transformers).
/// Eliminates duplicated GetCliOptions and BindOptions boilerplate.
/// </summary>
public abstract class BaseCliContributor : ICliContributor
{
    protected readonly OptionsRegistry Registry;
    private IEnumerable<Option>? _cliOptions;

    protected BaseCliContributor(OptionsRegistry registry)
    {
        Registry = registry;
    }

    public abstract string ProviderName { get; }
    public abstract string Category { get; }
    public abstract IEnumerable<Type> GetSupportedOptionTypes();

    public IEnumerable<Option> GetCliOptions()
    {
        return _cliOptions ??= GetSupportedOptionTypes()
            .SelectMany(Cli.CliOptionBuilder.GenerateOptionsForType)
            .ToList();
    }

    public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
    {
        var options = GetCliOptions();
        foreach (var type in GetSupportedOptionTypes())
        {
            var boundOptions = Cli.CliOptionBuilder.BindForType(type, parseResult, options);
            registry.RegisterByType(type, boundOptions);
        }
    }

    public virtual Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default)
    {
        return Task.FromResult<int?>(null);
    }
}
