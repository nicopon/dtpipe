using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Arrow.Mask;

public class MaskDataTransformerFactory : TransformerFactoryBase<MaskOptions>
{
	public MaskDataTransformerFactory(OptionsRegistry registry) : base(registry) { }

	public override string ComponentName => "mask";


	public override string Category => "Transformers";
	public override Type OptionsType => typeof(DtPipe.Transformers.Arrow.Mask.MaskOptions);

	protected override IDataTransformer? CreateFromTypedOptions(MaskOptions options)
	{
		return new MaskDataTransformer(options);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		// Get config options (like SkipNull) from registry-bound options
		var registryOptions = Registry.Get<MaskOptions>();

		var options = new DtPipe.Transformers.Arrow.Mask.MaskOptions
		{
			Mask = [.. configuration.Select(x => x.Value)],
			SkipNull = registryOptions.SkipNull
		};
		return new MaskDataTransformer(options);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// A mapping is column → pattern; an empty value means the default mask for that column.
		if (config.Mappings is not { Count: > 0 }) return null;

		return new MaskOptions
		{
			Mask = config.Mappings.Select(kvp => string.IsNullOrEmpty(kvp.Value) ? kvp.Key : $"{kvp.Key}:{kvp.Value}").ToList()
		};
	}
}
