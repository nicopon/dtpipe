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
    FlagStage Stage = FlagStage.All);

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
}
