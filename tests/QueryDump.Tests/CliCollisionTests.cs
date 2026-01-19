using System.CommandLine;
using System.Reflection;
using FluentAssertions;
using QueryDump.Cli;
using QueryDump.Core.Options;
using QueryDump.Core;
using Xunit;

namespace QueryDump.Tests;

public class CliCollisionTests
{
    [Fact]
    public void DetectCollisions_ShouldHaveUniqueOptions()
    {
        // 1. Gather all implemented ICliContributors
        var assembly = typeof(ICliContributor).Assembly;
        var contributorTypes = assembly.GetTypes()
            .Where(t => typeof(ICliContributor).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract)
            .ToList();

        var optionsRegistry = new OptionsRegistry();
        
        // 2. Instantiate and get options
        var allOptions = new List<(string Source, Option Option)>();

        // Add Core Options manually (mirrors CliService build logic)
        // Note: If you add core options, update this list!
        var coreOptions = new List<Option>
        {
            new Option<string?>("--connection", "-c"),
            new Option<string>("--provider", "-p"),
            new Option<string?>("--query", "-q"),
            new Option<string?>("--output", "-o"),
            new Option<int>("--connection-timeout"),
            new Option<int>("--query-timeout"),
            new Option<int>("--batch-size", "-b"),
            // Help and Version are standard sys-commandline but included for completeness if customized
            new Option<bool>("--help", "-h", "-?"),
            new Option<bool>("--version")
        };
        
        foreach(var opt in coreOptions) allOptions.Add(("Core", opt));

        foreach (var type in contributorTypes)
        {
            object instance;
            // Most factories take (OptionsRegistry).
            try 
            {
                instance = Activator.CreateInstance(type, optionsRegistry)!;
            }
            catch(MissingMethodException)
            {
                 try 
                 {
                     instance = Activator.CreateInstance(type)!;
                 } 
                 catch (Exception ex)
                 {
                     throw new InvalidOperationException($"Could not instantiate contributor {type.Name} for collision test. Does it have a complex constructor?", ex);
                 }
            }
            
            if (instance is ICliContributor contributor)
            {
                foreach (var opt in contributor.GetCliOptions())
                {
                    allOptions.Add((type.Name, opt));
                }
            }
        }

        // 3. Check for duplicates in Names and Aliases
        var definedAliases = new Dictionary<string, List<string>>(); // Alias/Name -> List of Sources

        foreach (var (source, opt) in allOptions)
        {
            HashSet<string> namesToCheck = [opt.Name, .. opt.Aliases];

            foreach (var name in namesToCheck)
            {
                if (!definedAliases.TryGetValue(name, out var sources))
                {
                    sources = [];
                    definedAliases[name] = sources;
                }
                sources.Add(source);
            }
        }

        // 4. Trace collisions
        // We only care if the list of sources has > 1 DISTINCT elements.
        // (A source defining the same option name twice is also weird but System.CommandLine might handle it, 
        //  but sharing between different sources is a collision)
        var collisions = definedAliases
                                .Where(kv => kv.Value.Distinct().Count() > 1)
                                .Select(kv => $"Option '{kv.Key}' is claimed by multiple sources: {string.Join(", ", kv.Value.Distinct())}")
                                .ToList();

        collisions.Should().BeEmpty("CLI options (names or aliases) must be unique across all contributors and core options.");
    }
}
