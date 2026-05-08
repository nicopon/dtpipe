using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
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
		=> _inner.CreateFromConfiguration(configuration);

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
		=> _inner.CreateFromYamlConfig(config);

	public IDataTransformer? CreateFromOptions(object options)
		=> _inner.CreateFromOptions(options);

	public IEnumerable<FlagDef> GetFlagDefs()
		=> CliOptionBuilder.GenerateFlagDefsForType(OptionsType);
}
