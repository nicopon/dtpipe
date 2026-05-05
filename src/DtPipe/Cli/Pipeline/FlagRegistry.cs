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

public record FlagDef(
    string Name,
    string[] Aliases,
    FlagArity Arity,
    FlagScope Scope,
    string? Description = null);

public class FlagRegistry
{
    private readonly Dictionary<string, FlagDef> _flags = new(StringComparer.OrdinalIgnoreCase);

    public void Register(FlagDef def)
    {
        _flags[def.Name] = def;
        foreach (var alias in def.Aliases)
        {
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
