using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Dynamically builds a transformer pipeline from CLI arguments.
/// Matches arguments to registered transformer factories and groups their options.
/// </summary>
public class TransformerPipelineBuilder
{
    private readonly IEnumerable<IDataTransformerFactory> _factories;

    public TransformerPipelineBuilder(IEnumerable<IDataTransformerFactory> factories)
    {
        _factories = factories;
    }

    public List<IDataTransformer> Build(string[] args)
    {
        var pipeline = new List<IDataTransformer>();
        var transformerFactories = _factories.ToList();

        // Build option map (option alias -> (Factory, Option) tuple)
        var optionMap = new Dictionary<string, (IDataTransformerFactory Factory, Option Option)>(StringComparer.OrdinalIgnoreCase);
        foreach (var factory in transformerFactories)
        {
            var options = (factory is ICliContributor contributor)
                ? contributor.GetCliOptions()
                : CliOptionBuilder.GenerateOptionsForType(factory.OptionsType);

            foreach (var option in options)
            {
                if (!string.IsNullOrEmpty(option.Name))
                    optionMap[option.Name] = (factory, option);
                foreach (var alias in option.Aliases)
                    optionMap[alias] = (factory, option);
            }
        }

        // Group consecutive args by transformer type
        IDataTransformerFactory? currentFactory = null;
        var currentOptions = new List<(string Key, string Value)>();

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (optionMap.TryGetValue(arg, out var match))
            {
                var factory = match.Factory;
                var option = match.Option;

                if (factory != currentFactory && currentFactory != null && currentOptions.Count > 0)
                {
                    var transformer = currentFactory.CreateFromConfiguration(currentOptions);
                    if (transformer != null) pipeline.Add(transformer);
                    currentOptions.Clear();
                }

                currentFactory = factory;

                // Determine if we should consume a value
                string? value = null;
                if (option.Arity.MaximumNumberOfValues > 0)
                {
                    if (i + 1 < args.Length)
                    {
                        value = args[i + 1];
                        i++;
                    }
                }
                else
                {
                    // Flag/Boolean option (Arity 0)
                    value = "true";
                }

                if (value != null)
                {
                    var optionName = arg.TrimStart('-');
                    var componentName = factory.ComponentName;

                    // Support both --type "col:val" and --type-option "val"
                    if (optionName.Equals(componentName, StringComparison.OrdinalIgnoreCase))
                    {
                        // It's the primary mapping option (e.g. --fake "NAME:faker")
                        currentOptions.Add((arg, value));
                    }
                    else
                    {
                        // It's a specific option (e.g. --fake-locale "fr")
                        currentOptions.Add((arg, value));
                    }
                }
            }
        }

        // Flush last
        if (currentFactory != null && currentOptions.Count > 0)
        {
            var transformer = currentFactory.CreateFromConfiguration(currentOptions);
            if (transformer != null) pipeline.Add(transformer);
        }

        return pipeline;
    }
}
