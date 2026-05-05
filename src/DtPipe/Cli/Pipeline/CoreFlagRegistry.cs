using System.Collections.Generic;

namespace DtPipe.Cli.Pipeline;

public static class CoreFlagRegistry
{
    public static void RegisterCoreFlags(FlagRegistry registry)
    {
        // Strictly Global Flags
        registry.Register(new FlagDef("--dry-run", new[] { "-dr" }, FlagArity.Scalar, FlagScope.Global, "Dry-run mode (N rows)"));
        registry.Register(new FlagDef("--no-stats", new string[] { }, FlagArity.Boolean, FlagScope.Global, "Disable progress and statistics"));
        registry.Register(new FlagDef("--log", new string[] { }, FlagArity.Scalar, FlagScope.Global, "Log file path"));
        registry.Register(new FlagDef("--job", new[] { "-j" }, FlagArity.Scalar, FlagScope.Global, "YAML job file path"));
        registry.Register(new FlagDef("--export-job", new string[] { }, FlagArity.Scalar, FlagScope.Global, "Export current CLI as YAML job file"));
        registry.Register(new FlagDef("--metrics-path", new string[] { }, FlagArity.Scalar, FlagScope.Global, "Path to save JSON metrics"));

        // Per-Branch Flags (can also be used at start as defaults)
        registry.Register(new FlagDef("--input", new[] { "-i" }, FlagArity.Scalar, FlagScope.PerBranch, "Input connection string"));
        registry.Register(new FlagDef("--output", new[] { "-o" }, FlagArity.Scalar, FlagScope.PerBranch, "Output connection string"));
        registry.Register(new FlagDef("--query", new[] { "-q" }, FlagArity.Scalar, FlagScope.PerBranch, "SQL query or file path"));
        registry.Register(new FlagDef("--table", new[] { "-t" }, FlagArity.Scalar, FlagScope.PerBranch, "Target table name"));
        registry.Register(new FlagDef("--strategy", new[] { "-s" }, FlagArity.Scalar, FlagScope.PerBranch, "Write strategy (Recreate, Append, Upsert, etc.)"));
        registry.Register(new FlagDef("--insert-mode", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Insert mode (Standard, Bulk, Binary)"));
        registry.Register(new FlagDef("--alias", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Alias for the current branch"));
        registry.Register(new FlagDef("--from", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Source alias(es) for this branch"));
        // --sql and --merge are contributed by their respective IStreamTransformerFactory via CliTriggerFlags
        registry.Register(new FlagDef("--ref", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Reference alias for JOINs"));
        registry.Register(new FlagDef("--schema-save", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Save discovered schema to file"));
        registry.Register(new FlagDef("--schema-load", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Load schema from file"));
        registry.Register(new FlagDef("--path", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Navigation path (JSON/XML)"));
        registry.Register(new FlagDef("--column-types", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Explicit column types"));
        registry.Register(new FlagDef("--auto-column-types", new string[] { }, FlagArity.Boolean, FlagScope.PerBranch, "Infer column types from sample"));
        registry.Register(new FlagDef("--max-sample", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Max rows to sample for inference"));
        registry.Register(new FlagDef("--encoding", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "File encoding (UTF-8, etc.)"));
        registry.Register(new FlagDef("--connection-timeout", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Connection timeout in seconds"));
        registry.Register(new FlagDef("--query-timeout", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Query timeout in seconds"));
        registry.Register(new FlagDef("--unsafe-query", new string[] { }, FlagArity.Boolean, FlagScope.PerBranch, "Allow unsafe SQL queries"));
        registry.Register(new FlagDef("--strict-schema", new string[] { }, FlagArity.Boolean, FlagScope.PerBranch, "Fail if schema mismatch"));
        registry.Register(new FlagDef("--no-schema-validation", new string[] { }, FlagArity.Boolean, FlagScope.PerBranch, "Skip target schema validation"));
        registry.Register(new FlagDef("--auto-migrate", new string[] { }, FlagArity.Boolean, FlagScope.PerBranch, "Auto-add missing columns to target"));
        registry.Register(new FlagDef("--pre-exec", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Script to run before export"));
        registry.Register(new FlagDef("--post-exec", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Script to run after export"));
        registry.Register(new FlagDef("--on-error-exec", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Script to run on error"));
        registry.Register(new FlagDef("--finally-exec", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Script to run finally"));
        // --rename and --drop are contributed by ProjectDataTransformerFactory via GetFlagDefs()
        // --throttle and --ignore-nulls were never implemented — removed
        
        // Overridable Core Flags
        registry.Register(new FlagDef("--key", new[] { "-k" }, FlagArity.Scalar, FlagScope.PerBranch, "Primary key for upsert/delete"));
        registry.Register(new FlagDef("--limit", new string[] { }, FlagArity.Scalar, FlagScope.PerBranch, "Limit total rows to process"));
        registry.Register(new FlagDef("--batch-size", new[] { "-b" }, FlagArity.Scalar, FlagScope.PerBranch, "Batch size for processing"));
        registry.Register(new FlagDef("--sampling-rate", new[] { "--sample-rate" }, FlagArity.Scalar, FlagScope.PerBranch, "Sampling rate (0.0 to 1.0)"));
        registry.Register(new FlagDef("--sampling-seed", new[] { "--sample-seed" }, FlagArity.Scalar, FlagScope.PerBranch, "Seed for deterministic sampling"));
        registry.Register(new FlagDef("--prefix", new[] { "-p" }, FlagArity.Scalar, FlagScope.PerBranch, "Prefix for all output tables"));
    }
}
