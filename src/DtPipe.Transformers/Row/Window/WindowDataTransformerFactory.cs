using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

using DtPipe.Transformers.Abstract;

namespace DtPipe.Transformers.Row.Window;

public class WindowDataTransformerFactory : TransformerFactoryBase<WindowOptions>
{

	public override string ComponentName => "window";
	private readonly IJsEngineProvider _jsEngineProvider;

	public WindowDataTransformerFactory(IJsEngineProvider jsEngineProvider)
	{
		_jsEngineProvider = jsEngineProvider;
	}

	public override string Category => "Transformers";

	protected override IDataTransformer? CreateFromTypedOptions(WindowOptions options)
	{
		return new WindowDataTransformer(options, _jsEngineProvider);
	}

	public override IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var options = new DtPipe.Transformers.Row.Window.WindowOptions();

		foreach (var (key, val) in configuration)
		{
			if (key.Equals("window-count", StringComparison.OrdinalIgnoreCase) || key.Equals("--window-count", StringComparison.OrdinalIgnoreCase) || key.Equals("count", StringComparison.OrdinalIgnoreCase))
			{
				if (int.TryParse(val, out var c)) options.Count = c;
			}
			else if (key.Equals("window-key", StringComparison.OrdinalIgnoreCase) || key.Equals("--window-key", StringComparison.OrdinalIgnoreCase) || key.Equals("key", StringComparison.OrdinalIgnoreCase))
			{
				options.Key = val;
			}
			else if (key.Equals("window-script", StringComparison.OrdinalIgnoreCase) || key.Equals("--window-script", StringComparison.OrdinalIgnoreCase) || key.Equals("script", StringComparison.OrdinalIgnoreCase))
			{
				options.Script = val;
			}
		}

		return new WindowDataTransformer(options, _jsEngineProvider);
	}

	public override object? CreateOptionsFromYaml(TransformerConfig config)
	{
		// Mapping values are script lines; the keys carry nothing for this transformer. An explicit
		// 'script' option binds after this and wins.
		var options = new WindowOptions();
		if (config.Mappings is { Count: > 0 })
			options.Script = string.Join("\n", config.Mappings.Select(kvp =>
				string.IsNullOrEmpty(kvp.Value) ? kvp.Key : kvp.Value));

		return options;
	}
}
