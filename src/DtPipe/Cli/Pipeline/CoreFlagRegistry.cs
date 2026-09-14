namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Registers only the structural CLI flags whose semantics are wired into the lexer itself
/// (branch-splitting triggers, DAG routing, global meta). All other flags are contributed
/// dynamically by their respective components via ICliContributor.GetFlagDefs().
/// Core flags are FlagStage.All — they are valid in any stage position.
/// </summary>
public static class CoreFlagRegistry
{
    public static void RegisterCoreFlags(FlagRegistry registry)
    {
        // DAG structure — lexer uses these names explicitly for branch-split logic
        registry.Register(new FlagDef("--input",  new[] { "-i" }, FlagArity.Scalar, FlagScope.PerBranch, "Input source (prefixed ADO.NET, file, or '-')",  FlagStage.All));
        registry.Register(new FlagDef("--output", new[] { "-o" }, FlagArity.Scalar, FlagScope.PerBranch, "Output target (prefixed ADO.NET, file, or '-')", FlagStage.All));
        registry.Register(new FlagDef("--from",   new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Source alias(es) for this branch", FlagStage.All));
        registry.Register(new FlagDef("--alias",  new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Alias for the current branch",     FlagStage.All));
        // Scalar, like every other value flag: an alias list is written with commas
        // (--ref a,b). The grammar gives repetition exactly one meaning — opening a branch, as
        // -i/--from/--job do — and a flag that also accumulated on repeat would teach that
        // '--from a --from b' adds a source, when it starts a second branch instead.
        registry.Register(new FlagDef("--ref",    new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Reference alias(es) for JOINs, comma-separated", FlagStage.All));
        registry.Register(new FlagDef("--job",    new[] { "-j" },  FlagArity.Scalar, FlagScope.Global,    "YAML job file path",               FlagStage.All));

        // Global meta flags
        registry.Register(new FlagDef("--dry-run",     new[] { "-dr" },  FlagArity.Scalar,  FlagScope.Global, "Dry-run mode (N rows)",                  FlagStage.All));
        registry.Register(new FlagDef("--no-stats",    new string[] { }, FlagArity.Boolean, FlagScope.Global, "Disable progress and statistics",        FlagStage.All));
        registry.Register(new FlagDef("--log",         new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Log file path",                          FlagStage.All));
        registry.Register(new FlagDef("--export-job",  new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Export current CLI as YAML job file",    FlagStage.All));
        registry.Register(new FlagDef("--metrics-path",new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Path to save JSON metrics",              FlagStage.All));
        registry.Register(new FlagDef("--strict-bindings", new string[] { }, FlagArity.Boolean, FlagScope.Global, "Fail (exit non-zero) on an undeclared flag that consumes no value, or an unknown job-file option key", FlagStage.All));

        // Incremental cursor flags
        registry.Register(new FlagDef("--cursor",      new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Cursor column for incremental loading (writer-side)", FlagStage.All));
        registry.Register(new FlagDef("--state",       new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "State file path for cursor persistence (writer-side)", FlagStage.All));
        registry.Register(new FlagDef("--cursor-from", new string[] { }, FlagArity.Scalar, FlagScope.Global,    "Override cursor value for this run",                   FlagStage.All));

        // Session — names the store that materialised artefacts belong to. Scalar like every
        // other value flag: repetition means "open a branch" in this grammar, and only for
        // -i / --from / --job.
        registry.Register(new FlagDef("--session",     new string[] { }, FlagArity.Scalar, FlagScope.Global,    "Name the session materialised artefacts belong to",    FlagStage.All));

        // Materialisation. Per-branch, and neither splits a branch: only -i, --from and --job do.
        registry.Register(new FlagDef("--checkpoint",      new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Materialise this branch's output in the session store", FlagStage.All));
        registry.Register(new FlagDef("--from-checkpoint", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Resume this branch from a stored checkpoint",           FlagStage.All));
    }
}
