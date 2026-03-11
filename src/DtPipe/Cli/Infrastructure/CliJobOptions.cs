using System.CommandLine;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Aggregates all CLI Option objects passed to RawJobBuilder.
/// Avoids a 25-parameter method signature.
/// </summary>
public sealed class CliJobOptions
{
    public required Option<string?> Job { get; init; }
    public required Option<string[]> Input { get; init; }
    public required Option<string> Query { get; init; }
    public required Option<string[]> Output { get; init; }
    public required Option<int> ConnectionTimeout { get; init; }
    public required Option<int> QueryTimeout { get; init; }
    public required Option<int> BatchSize { get; init; }
    public required Option<bool> UnsafeQuery { get; init; }
    public required Option<bool> NoStats { get; init; }
    public required Option<int> Limit { get; init; }
    public required Option<double> SamplingRate { get; init; }
    public required Option<int?> SamplingSeed { get; init; }
    public required Option<string?> Log { get; init; }
    public required Option<string> Key { get; init; }
    public required Option<string> PreExec { get; init; }
    public required Option<string> PostExec { get; init; }
    public required Option<string> OnErrorExec { get; init; }
    public required Option<string> FinallyExec { get; init; }
    public required Option<string> Strategy { get; init; }
    public required Option<string> InsertMode { get; init; }
    public required Option<string> Table { get; init; }
    public required Option<int> MaxRetries { get; init; }
    public required Option<int> RetryDelayMs { get; init; }
    public required Option<bool?> StrictSchema { get; init; }
    public required Option<bool?> NoSchemaValidation { get; init; }
    public required Option<string?> MetricsPath { get; init; }
    public required Option<string?> Prefix { get; init; }
    public required Option<bool?> AutoMigrate { get; init; }
    public required Option<string[]> Xstreamer { get; init; }
    public required Option<string?> ExportJob { get; init; }
    public required Option<string[]> Rename { get; init; }
    public required Option<string[]> Drop { get; init; }
    public required Option<int> Throttle { get; init; }
    public required Option<bool> IgnoreNulls { get; init; }
}
