using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Options;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class CliCollisionTests
{
    [Fact]
    public void DetectCollisions_ShouldHaveUniqueFlags()
    {
        // 1. Gather all implemented ICliContributors
        var assembly = typeof(ICliContributor).Assembly;
        var contributorTypes = assembly.GetTypes()
            .Where(t => typeof(ICliContributor).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract && !t.ContainsGenericParameters)
            .Where(t => t.Name != "CliDataWriterFactory" && t.Name != "CliStreamReaderFactory" && t.Name != "CliDataTransformerFactory" && t.Name != "CliProcessorFactory") // Skip infra wrappers requiring complex deps
            .ToList();

        var optionsRegistry = new OptionsRegistry();

        // 2. Collect all flags (name + aliases) from contributors
        var allFlags = new List<(string Source, string FlagName)>();

        foreach (var type in contributorTypes)
        {
            object instance;
            var ctor = type.GetConstructors().FirstOrDefault();
            if (ctor != null)
            {
                var args = new List<object?>();
                foreach (var param in ctor.GetParameters())
                {
                    if (param.ParameterType == typeof(OptionsRegistry))
                        args.Add(optionsRegistry);
                    else if (param.ParameterType == typeof(DtPipe.Transformers.Services.IJsEngineProvider))
                        args.Add(Moq.Mock.Of<DtPipe.Transformers.Services.IJsEngineProvider>());
                    else if (param.ParameterType == typeof(Microsoft.Extensions.Logging.ILoggerFactory))
                        args.Add(new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory());
                    else
                        args.Add(null);
                }

                try
                {
                    instance = ctor.Invoke(args.ToArray());
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Could not instantiate contributor {type.Name}.", ex);
                }
            }
            else
            {
                try
                {
                    instance = Activator.CreateInstance(type)!;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Could not instantiate contributor {type.Name}.", ex);
                }
            }

            if (instance is ICliContributor contributor)
            {
                foreach (var flag in contributor.GetFlagDefs())
                {
                    allFlags.Add((type.Name, flag.Name));
                    foreach (var alias in flag.Aliases)
                        allFlags.Add((type.Name, alias));
                }
            }
        }

        // 3. Check for duplicates: same flag name claimed by multiple distinct sources
        var definedAliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var (source, flagName) in allFlags)
        {
            if (!definedAliases.TryGetValue(flagName, out var sources))
            {
                sources = new List<string>();
                definedAliases[flagName] = sources;
            }
            sources.Add(source);
        }

        var collisions = definedAliases
            .Where(kv => kv.Value.Distinct().Count() > 1)
            .Select(kv => $"Flag '{kv.Key}' is claimed by multiple sources: {string.Join(", ", kv.Value.Distinct())}")
            .ToList();

        collisions.Should().BeEmpty("CLI flags (names or aliases) must be unique across all contributors.");
    }
}

[Collection("console-serial")]
public class UnmappedPropertyWarningTests
{
    /// <summary>
    /// F17: a property without any CLI metadata is skipped silently — it must warn once,
    /// then stay quiet for the remainder of the process.
    /// </summary>
    [Fact]
    public void Unmapped_Property_Warns_Once_At_Startup()
    {
        string firstRun;
        var originalError = Console.Error;
        var warmupSink = new StringWriter();
        var prevDebug = Environment.GetEnvironmentVariable("DEBUG");
        Environment.SetEnvironmentVariable("DEBUG", "1"); // skip-warnings are DEBUG-gated
        Console.SetError(warmupSink); // discard any pre-existing cache warm-up noise
        try
        {
            CliOptionBuilder.GenerateFlagDefsForType(typeof(OptionsWithBareProperty));
            firstRun = warmupSink.ToString();
        }
        finally
        {
            Console.SetError(originalError);
            if (prevDebug is null) Environment.SetEnvironmentVariable("DEBUG", null);
            else Environment.SetEnvironmentVariable("DEBUG", prevDebug);
        }
        Assert.Contains("UnmappedProperty", firstRun, StringComparison.Ordinal);

        var originalError2 = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            CliOptionBuilder.GenerateFlagDefsForType(typeof(OptionsWithBareProperty));
            CliOptionBuilder.GenerateFlagDefsForType(typeof(OptionsWithBareProperty));
        }
        finally
        {
            Console.SetError(originalError2);
        }

        Assert.DoesNotContain("UnmappedProperty", captured.ToString(), StringComparison.Ordinal);
    }

    private sealed class OptionsWithBareProperty
    {
        public static string Prefix => "bare";
        public static string DisplayName => "Bare";

        [DtPipe.Core.Attributes.ComponentOption("--known")]
        public string Known { get; set; } = "";

        public string UnmappedProperty { get; set; } = "";
    }
}
