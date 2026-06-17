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
        registry.Register(new FlagDef("--input",  new[] { "-i" }, FlagArity.Scalar, FlagScope.PerBranch, "Input connection string",  FlagStage.All));
        registry.Register(new FlagDef("--output", new[] { "-o" }, FlagArity.Scalar, FlagScope.PerBranch, "Output connection string", FlagStage.All));
        registry.Register(new FlagDef("--from",   new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Source alias(es) for this branch", FlagStage.All));
        registry.Register(new FlagDef("--alias",  new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Alias for the current branch",     FlagStage.All));
        registry.Register(new FlagDef("--ref",    new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Reference alias for JOINs",        FlagStage.All));
        registry.Register(new FlagDef("--job",    new[] { "-j" },  FlagArity.Scalar, FlagScope.Global,    "YAML job file path",               FlagStage.All));

        // Global meta flags
        registry.Register(new FlagDef("--dry-run",     new[] { "-dr" },  FlagArity.Scalar,  FlagScope.Global, "Dry-run mode (N rows)",                  FlagStage.All));
        registry.Register(new FlagDef("--no-stats",    new string[] { }, FlagArity.Boolean, FlagScope.Global, "Disable progress and statistics",        FlagStage.All));
        registry.Register(new FlagDef("--log",         new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Log file path",                          FlagStage.All));
        registry.Register(new FlagDef("--export-job",  new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Export current CLI as YAML job file",    FlagStage.All));
        registry.Register(new FlagDef("--metrics-path",new string[] { }, FlagArity.Scalar,  FlagScope.Global, "Path to save JSON metrics",              FlagStage.All));

        // Incremental cursor flags
        registry.Register(new FlagDef("--cursor",      new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Cursor column for incremental loading (writer-side)", FlagStage.All));
        registry.Register(new FlagDef("--state",       new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "State file path for cursor persistence (writer-side)", FlagStage.All));
        registry.Register(new FlagDef("--cursor-from", new string[] { }, FlagArity.Scalar, FlagScope.Global,    "Override cursor value for this run",                   FlagStage.All));
    }
}
