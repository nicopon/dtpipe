using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Row.Window;

public class WindowDataTransformerFactory : IDataTransformerFactory
{
	public bool CanHandle(string connectionString) => false;
	private readonly IJsEngineProvider _jsEngineProvider;

	public WindowDataTransformerFactory(IJsEngineProvider jsEngineProvider)
	{
		_jsEngineProvider = jsEngineProvider;
	}

	public string Category => "Transformers";
	public string ComponentName => "window";
	public Type OptionsType => typeof(DtPipe.Transformers.Row.Window.WindowOptions);

	public IDataTransformer CreateFromOptions(DtPipe.Transformers.Row.Window.WindowOptions options)
	{
		return new WindowDataTransformer(options, _jsEngineProvider);
	}

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
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

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
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
		if (config.Compute != null)
		{
			// Join lines? Or take first? Window script should be single string.
			options.Script = string.Join("\n", config.Compute);
		}
		else if (config.Options != null && config.Options.TryGetValue("script", out var scriptVal))
		{
			options.Script = scriptVal;
		}

		return new WindowDataTransformer(options, _jsEngineProvider);
	}
}
