using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Carries the resolved (prefix-stripped) connection strings for the reader and writer of a branch.
/// Registered in the OptionsRegistry by LinearPipelineService so that CliStreamReaderFactory and
/// CliDataWriterFactory can resolve routing data without coupling PipelineOptions to adapter concerns.
/// </summary>
public class ConnectionRoute : IOptionSet
{
    public static string Prefix => "route";
    public static string DisplayName => "Connection Route";

    public string Input { get; init; } = string.Empty;
    public string Output { get; init; } = string.Empty;

    public ConnectionRoute() { }
    public ConnectionRoute(string input, string output)
    {
        Input = input;
        Output = output;
    }
}
