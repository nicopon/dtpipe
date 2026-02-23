using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Expand;

public class ExpandDataTransformerFactory : IDataTransformerFactory
{
	private readonly OptionsRegistry _registry;
	private readonly IJsEngineProvider _jsEngineProvider;
	public string Category => "Transformers";
	public Type OptionsType => typeof(ExpandOptions);

	public ExpandDataTransformerFactory(OptionsRegistry registry, IJsEngineProvider jsEngineProvider)
	{
		_registry = registry;
		_jsEngineProvider = jsEngineProvider;
	}

	public string ComponentName => "expand";

	public bool CanHandle(string connectionString) => false;

	public IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration)
	{
		var expands = new List<string>();

		foreach (var (option, value) in configuration)
		{
			if (string.Equals(option, "expand", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(option, "--expand", StringComparison.OrdinalIgnoreCase))
			{
				if (value.StartsWith("@"))
				{
					var filePath = value.Substring(1);
					if (File.Exists(filePath))
					{
						expands.Add(File.ReadAllText(filePath));
					}
					else
					{
						expands.Add(value);
					}
				}
				else
				{
					expands.Add(value);
				}
			}
		}

		if (expands.Count == 0) return new ExpandDataTransformer(new ExpandOptions(), _jsEngineProvider);

		return new ExpandDataTransformer(new ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}

	public IDataTransformer? CreateFromYamlConfig(TransformerConfig config)
	{
		var expands = new List<string>();

		if (config.Compute != null)
		{
			expands.AddRange(config.Compute.Values);
		}

		if (expands.Count == 0) return null;

		return new ExpandDataTransformer(new ExpandOptions { Expand = expands.ToArray() }, _jsEngineProvider);
	}
}
