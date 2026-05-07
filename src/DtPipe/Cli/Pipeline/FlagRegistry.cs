using System;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Cli.Pipeline;

public enum FlagArity
{
    Boolean,
    Scalar,
    Repeatable
}

public enum FlagScope
{
    Global,
    PerBranch
}

/// <summary>
/// Bitmask indicating in which pipeline stage a flag is valid.
/// Flags registered by both reader and writer contributors accumulate bits via merge-on-register.
/// </summary>
[Flags]
public enum FlagStage
{
    Reader   = 1,          // valid in reader scope (before first transformer trigger or -o)
    Pipeline = 2,          // valid in transformer scope (transformer triggers + transformer options)
    Writer   = 4,          // valid in writer scope (after -o)
    Any      = Reader | Writer,         // valid in reader and writer, but NOT pipeline (e.g. --table, --query, --key)
    All      = Reader | Pipeline | Writer  // valid anywhere (engine controls, DAG routing flags)
}

public record FlagDef(
    string Name,
    string[] Aliases,
    FlagArity Arity,
    FlagScope Scope,
    string? Description = null,
    FlagStage Stage = FlagStage.All,
    string? ComponentName = null);

public class FlagRegistry
{
    private readonly Dictionary<string, FlagDef> _flags = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Registers a flag definition. If the flag name is already registered, the Stage bitmasks
    /// are merged (OR) so that a flag contributed by both a reader and a writer contributor ends
    /// up as FlagStage.Any = Reader | Writer.
    /// </summary>
    public void Register(FlagDef def)
    {
        if (_flags.TryGetValue(def.Name, out var existing))
            def = def with { Stage = existing.Stage | def.Stage };

        _flags[def.Name] = def;
        foreach (var alias in def.Aliases)
        {
            if (_flags.TryGetValue(alias, out var existingAlias))
                def = def with { Stage = existingAlias.Stage | def.Stage };
            _flags[alias] = def;
        }
    }

    public FlagDef? Lookup(string token)
    {
        if (string.IsNullOrEmpty(token)) return null;
        return _flags.TryGetValue(token, out var def) ? def : null;
    }

    public IEnumerable<FlagDef> GetAll() => _flags.Values.Distinct();

    /// <summary>
    /// Performs post-registration quality control to detect collisions and inconsistencies.
    /// Throws InvalidOperationException if issues are found.
    /// </summary>
    public void Validate()
    {
        var allFlags = new Dictionary<string, List<FlagDef>>(StringComparer.OrdinalIgnoreCase);
        foreach (var def in GetAll())
        {
            AddToMap(allFlags, def.Name, def);
            foreach (var alias in def.Aliases) AddToMap(allFlags, alias, def);
        }

        foreach (var kvp in allFlags)
        {
            var flagName = kvp.Key;
            var defs = kvp.Value;

            if (defs.Count <= 1) continue;

            // 1. Check for overlapping stages between different components
            for (int i = 0; i < defs.Count; i++)
            {
                for (int j = i + 1; j < defs.Count; j++)
                {
                    var a = defs[i];
                    var b = defs[j];

                    if (a.ComponentName == b.ComponentName) continue;

                    // Detect overlap in stages
                    var overlap = a.Stage & b.Stage;
                    if (overlap != 0)
                    {
                        throw new InvalidOperationException(
                            $"CLI Flag Collision: Flag '{flagName}' is registered by multiple components ({a.ComponentName}, {b.ComponentName}) " +
                            $"for overlapping stages ({overlap}). Flags must be unique within a stage (especially in the Pipeline/Transformer stage).");
                    }

                    // 2. Check for Arity inconsistencies if shared across stages (e.g. Reader/Writer)
                    if (a.Arity != b.Arity)
                    {
                        throw new InvalidOperationException(
                            $"CLI Arity Mismatch: Flag '{flagName}' has inconsistent arity between components ({a.ComponentName}: {a.Arity}, {b.ComponentName}: {b.Arity}).");
                    }
                }
            }
        }
    }

    private static void AddToMap(Dictionary<string, List<FlagDef>> map, string flag, FlagDef def)
    {
        if (!map.ContainsKey(flag)) map[flag] = new List<FlagDef>();
        map[flag].Add(def);
    }
}
