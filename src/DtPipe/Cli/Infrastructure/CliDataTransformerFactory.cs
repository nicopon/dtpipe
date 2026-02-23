using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

public class CliDataTransformerFactory : IDataTransformerFactory, ICliContributor
{
	private readonly IDataTransformerFactory _inner;

	public CliDataTransformerFactory(IDataTransformerFactory inner)
	{
		_inner = inner;
	}

	public string ComponentName => _inner.ComponentName;
	public string Category => _inner.Category;
	public Type OptionsType => _inner.OptionsType;

	public bool CanHandle(string connectionString) => _inner.CanHandle(connectionString);

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		return _inner.CreateFromConfiguration(configuration);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		return _inner.CreateFromYamlConfig(config);
	}

	public IEnumerable<Option> GetCliOptions()
	{
		return CliOptionBuilder.GenerateOptionsForType(OptionsType);
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();
		var existingOptions = registry.Get(OptionsType);
		CliOptionBuilder.BindForType(OptionsType, existingOptions, parseResult, options, null);
		registry.RegisterByType(OptionsType, existingOptions);
	}
}
