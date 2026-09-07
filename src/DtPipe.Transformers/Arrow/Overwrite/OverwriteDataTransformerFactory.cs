using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Arrow.Overwrite;

public class OverwriteDataTransformerFactory : TransformerFactoryBase<OverwriteOptions>
{

	public override string ComponentName => "overwrite";

	public OverwriteDataTransformerFactory(OptionsRegistry registry) : base(registry) { }



	public override string Category => "Transformers";


	protected override IDataTransformer? CreateFromTypedOptions(OverwriteOptions options)
	{
		return new OverwriteDataTransformer(options);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = Registry.Get<OverwriteOptions>();

		var options = new DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions
		{
			Overwrite = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
		return new OverwriteDataTransformer(options);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// An empty value means the key already carries its own separator ("Col=Val").
		if (config.Mappings is not { Count: > 0 }) return null;

		return new DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions
		{
			Overwrite = config.Mappings.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}").ToList()
		};
	}
}
