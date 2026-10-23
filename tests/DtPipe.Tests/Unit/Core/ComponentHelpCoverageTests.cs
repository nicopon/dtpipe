using System.ComponentModel;
using System.Reflection;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class ComponentHelpCoverageTests
{
    [Fact]
    public void AllRegisteredOptionsTypes_HaveDescriptionAndComponentHelp()
    {
        // Anchor types force-load the two assemblies holding every reader/writer descriptor and
        // every transformer factory. IStreamTransformerFactory (sql/merge) does not implement
        // IComponentDescriptor and has no OptionsType, so it is correctly out of scope here.
        var assemblies = new[]
        {
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Transformers.Row.Compute.ComputeDataTransformerFactory).Assembly,
        };

        var componentTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IComponentDescriptor).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract && !t.ContainsGenericParameters)
            .ToList();

        componentTypes.Count.Should().BeGreaterThanOrEqualTo(30, "the reflection scan should discover all registered readers, writers, and transformer factories");

        var optionsRegistry = new OptionsRegistry();
        var seenOptionsTypes = new HashSet<Type>();
        var failures = new List<string>();

        foreach (var componentType in componentTypes)
        {
            var ctor = componentType.GetConstructors().FirstOrDefault();
            if (ctor == null)
            {
                failures.Add($"{componentType.Name}: no public constructor found");
                continue;
            }

            var args = new List<object?>();
            foreach (var param in ctor.GetParameters())
            {
                if (param.ParameterType == typeof(OptionsRegistry))
                    args.Add(optionsRegistry);
                else if (param.ParameterType == typeof(IJsEngineProvider))
                    args.Add(Moq.Mock.Of<IJsEngineProvider>());
                else if (param.ParameterType == typeof(Microsoft.Extensions.Logging.ILoggerFactory))
                    args.Add(new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory());
                else
                    args.Add(null);
            }

            object instance;
            try
            {
                instance = ctor.Invoke(args.ToArray());
            }
            catch (Exception ex)
            {
                failures.Add($"{componentType.Name}: could not instantiate ({ex.Message})");
                continue;
            }

            var optionsType = ((IComponentDescriptor)instance).OptionsType;
            if (!seenOptionsTypes.Add(optionsType)) continue; // shared Options type across reader+writer (e.g. MemoryChannelOptions)

            var descriptionAttr = optionsType.GetCustomAttribute<DescriptionAttribute>();
            if (descriptionAttr == null || string.IsNullOrWhiteSpace(descriptionAttr.Description))
                failures.Add($"{optionsType.Name} (via {componentType.Name}): missing or empty [Description]");

            var helpAttr = optionsType.GetCustomAttribute<ComponentHelpAttribute>();
            if (helpAttr == null)
            {
                failures.Add($"{optionsType.Name} (via {componentType.Name}): missing [ComponentHelp]");
            }
            else
            {
                if (string.IsNullOrWhiteSpace(helpAttr.UsageNotes))
                    failures.Add($"{optionsType.Name} (via {componentType.Name}): [ComponentHelp] has empty UsageNotes");
                if (helpAttr.Examples.Length == 0 || helpAttr.Examples.All(string.IsNullOrWhiteSpace))
                    failures.Add($"{optionsType.Name} (via {componentType.Name}): [ComponentHelp] has no non-empty Examples");
            }
        }

        failures.Should().BeEmpty("every registered provider/transformer OptionsType must carry a non-empty [Description] and [ComponentHelp] so generated MCP/CLI help stays complete");
    }
}
