using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Arrow.Format;

public class FormatDataTransformerFactory : TransformerFactoryBase<FormatOptions>
{
	public FormatDataTransformerFactory(OptionsRegistry registry) : base(registry) { }

	public override string ComponentName => "format";


	public override string Category => "Transformers";
	public override Type OptionsType => typeof(DtPipe.Transformers.Arrow.Format.FormatOptions);

	protected override IDataTransformer? CreateFromTypedOptions(FormatOptions options)
	{
		return new FormatDataTransformer(options);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = Registry.Get<DtPipe.Transformers.Arrow.Format.FormatOptions>();

		var options = new DtPipe.Transformers.Arrow.Format.FormatOptions
		{
			Format = configuration.Select(x => x.Value),
			SkipNull = registryOptions.SkipNull
		};
		var configStr = string.Join(" | ", configuration.Select(x => $"{x.Option}={x.Value}"));

		return new FormatDataTransformer(options);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		if (config.Mappings is not { Count: > 0 }) return null;

		return new FormatOptions { Format = config.Mappings.Select(kvp => $"{kvp.Key}:{kvp.Value}").ToList() };
	}
}
