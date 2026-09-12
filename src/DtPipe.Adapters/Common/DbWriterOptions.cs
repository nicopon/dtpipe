using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base options for database writer adapters.
/// Declares shared CLI flags for schema validation, lifecycle hooks, and key-based operations.
/// </summary>
public abstract class DbWriterOptions : DbConnectionOptions, ISchemaValidationAware, IHookAware, IKeyAwareOptions
{
    [ComponentOption("--strict-schema", Description = "Freeze mode: fail if schema incompatibilities are detected. Combines with --auto-migrate; excludes --no-schema-validation")]
    public bool StrictSchema { get; set; } = false;

    [ComponentOption("--no-schema-validation", Description = "Disable pre-write schema compatibility validation. Excludes --strict-schema and --auto-migrate, which both need the inspection")]
    public bool NoSchemaValidation { get; set; } = false;

    [ComponentOption("--auto-migrate", Description = "Evolve mode: automatically add missing columns to target table. Combines with --strict-schema; excludes --no-schema-validation")]
    public bool AutoMigrate { get; set; } = false;

    [ComponentOption("--key", Aliases = new[] { "-k" }, Description = "Primary key column(s) for upsert/delete")]
    public string? Key { get; set; }

    [ComponentOption("--pre-exec", Description = "Script to run before data transfer")]
    public string? PreExec { get; set; }

    [ComponentOption("--post-exec", Description = "Script to run after successful data transfer")]
    public string? PostExec { get; set; }

    [ComponentOption("--on-error-exec", Description = "Script to run if an error occurs")]
    public string? OnErrorExec { get; set; }

    [ComponentOption("--finally-exec", Description = "Script to run always (finally)")]
    public string? FinallyExec { get; set; }
}
