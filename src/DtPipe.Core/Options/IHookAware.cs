namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by DB writer options that support SQL lifecycle hooks.
/// </summary>
public interface IHookAware
{
    string? PreExec { get; set; }
    string? PostExec { get; set; }
    string? OnErrorExec { get; set; }
    string? FinallyExec { get; set; }
}
