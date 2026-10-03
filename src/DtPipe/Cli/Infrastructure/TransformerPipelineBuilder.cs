using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Dynamically builds a transformer pipeline from CLI arguments.
/// Matches arguments to registered transformer factories and groups their options.
/// Uses FlagDef (from ICliContributor.GetFlagDefs()) instead of System.CommandLine Option.
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
        foreach (var (factory, pairs) in CollectGroups(args))
        {
            // Same binding path as the live CLI: bind the flag group into a fresh options
            // instance, then let the factory create the transformer from it.
            var instance = Activator.CreateInstance(factory.OptionsType)!;
            OptionBinder.BindPairs(instance, pairs);
            var transformer = factory.CreateFromOptions(instance);
            if (transformer != null) pipeline.Add(transformer);
        }
        return pipeline;
    }

    /// <summary>
    /// Groups raw args into ordered transformer flag groups using the canonical rule:
    /// consecutive options belonging to the same factory form one group, so a trailing option
    /// reaches every trigger value before it. A trigger that cannot hold a second value
    /// (non-repeatable) opens a new group of the same factory instead.
    /// A scalar option given twice in one group is accepted when both values agree and refused
    /// when they differ: one instance holds one value. A flag several factories declare binds
    /// only through the factory in context; anywhere else it is refused.
    /// Single source shared by live execution (<see cref="Build"/>) and --export-job.
    /// </summary>
    public List<(IDataTransformerFactory Factory, List<(string Option, string Value)> Pairs)> CollectGroups(string[] args)
    {
        var groups = new List<(IDataTransformerFactory, List<(string, string)>)>();
        var transformerFactories = _factories.ToList();

        // Build option maps
        var globalOptionMap = new Dictionary<string, (IDataTransformerFactory Factory, FlagDef Flag)>(StringComparer.OrdinalIgnoreCase);
        var flagOwners = new Dictionary<string, List<IDataTransformerFactory>>(StringComparer.OrdinalIgnoreCase);
        var perFactoryMap = new Dictionary<IDataTransformerFactory, Dictionary<string, FlagDef>>();

        foreach (var factory in transformerFactories)
        {
            var flags = (factory is ICliContributor contributor)
                ? contributor.GetFlagDefs()
                : CliOptionBuilder.GenerateFlagDefsForType(factory.OptionsType);

            var factoryDict = new Dictionary<string, FlagDef>(StringComparer.OrdinalIgnoreCase);
            perFactoryMap[factory] = factoryDict;

            foreach (var flag in flags)
            {
                if (!string.IsNullOrEmpty(flag.Name))
                {
                    globalOptionMap[flag.Name] = (factory, flag);
                    factoryDict[flag.Name] = flag;
                    AddOwner(flag.Name, factory);
                }
                foreach (var alias in flag.Aliases)
                {
                    globalOptionMap[alias] = (factory, flag);
                    factoryDict[alias] = flag;
                    AddOwner(alias, factory);
                }
            }
        }

        void AddOwner(string name, IDataTransformerFactory factory)
        {
            if (!flagOwners.TryGetValue(name, out var owners))
                flagOwners[name] = owners = new List<IDataTransformerFactory>();
            if (!owners.Contains(factory)) owners.Add(factory);
        }

        // Group consecutive args by transformer type
        IDataTransformerFactory? currentFactory = null;
        var currentOptions = new List<(string Key, string Value)>();

        void FlushCurrent()
        {
            if (currentFactory != null && currentOptions.Count > 0)
                groups.Add((currentFactory, currentOptions));
            currentOptions = new List<(string Key, string Value)>();
        }

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            // 1. Contextual lookup: prefer current factory if it supports the flag
            (IDataTransformerFactory Factory, FlagDef Flag)? match = null;
            if (currentFactory != null && perFactoryMap[currentFactory].TryGetValue(arg, out var currentFlag))
            {
                // Trigger Detection: a repeatable trigger extends the open instance, so the
                // options that follow reach every value. A trigger that holds one value only
                // starts a new instance of the same factory when it is already present.
                var triggerName = $"--{currentFactory.ComponentName.ToLowerInvariant()}";
                bool isTrigger = string.Equals(arg, triggerName, StringComparison.OrdinalIgnoreCase);

                if (isTrigger
                    && currentFlag.Arity != FlagArity.Repeatable
                    && currentOptions.Any(o => string.Equals(o.Key, arg, StringComparison.OrdinalIgnoreCase)))
                {
                    FlushCurrent();
                }

                match = (currentFactory, currentFlag);
            }
            // 2. Global lookup: a flag several factories declare has no owner outside its context.
            else if (flagOwners.TryGetValue(arg, out var owners) && owners.Count > 1)
            {
                throw new InvalidOperationException(
                    $"Flag '{arg}' is declared by several transformers ({string.Join(", ", owners.Select(o => $"--{o.ComponentName.ToLowerInvariant()}"))}). " +
                    "Place it right after a flag of the transformer it belongs to.");
            }
            else if (globalOptionMap.TryGetValue(arg, out var globalMatch))
            {
                match = globalMatch;
            }

            if (match != null)
            {
                var factory = match.Value.Factory;
                var flag = match.Value.Flag;

                if (factory != currentFactory && currentFactory != null && currentOptions.Count > 0)
                {
                    FlushCurrent();
                }

                currentFactory = factory;

                // Determine if we should consume a value — F8 arity-driven rule:
                // scalar/repeatable flags always take the next token, even dash-leading
                // (e.g. --mask "-###-").
                string? value = null;
                if (flag.ConsumesNextToken)
                {
                    if (i + 1 < args.Length)
                    {
                        value = args[++i];
                    }
                    else if (flag.Arity == FlagArity.Scalar)
                    {
                         // Missing required value for scalar
                         throw new InvalidOperationException($"Flag '{arg}' requires a value.");
                    }
                }
                else
                {
                    value = "true";
                }

                if (value != null)
                {
                    var heldIndex = flag.Arity == FlagArity.Repeatable
                        ? -1
                        : currentOptions.FindIndex(o => string.Equals(o.Key, arg, StringComparison.OrdinalIgnoreCase));

                    if (heldIndex < 0)
                        currentOptions.Add((arg, value));
                    else if (!string.Equals(currentOptions[heldIndex].Value, value, StringComparison.Ordinal))
                        throw new InvalidOperationException(
                            $"Flag '{arg}' is given twice with different values ('{currentOptions[heldIndex].Value}', '{value}') to the same --{factory.ComponentName.ToLowerInvariant()} step. " +
                            "Consecutive flags of one transformer form one step: separate the steps with another transformer's flag or a new branch (--from) to give them different values.");
                }
            }
        }

        // Flush last
        FlushCurrent();

        return groups;
    }
}
