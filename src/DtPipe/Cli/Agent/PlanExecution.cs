using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Running the validated plan, as one policy: state what is about to run, ask once more only when
/// the launch already consented to a real write (<c>--apply</c>), then hand the YAML to the engine
/// — never back through the model. The two surfaces differ only in how they show and how they ask;
/// that they show and ask at all is decided here, once.
///
/// <para>
/// The scrollback path rendered the DAG before its question and let a tool exception kill the
/// process; the full-screen modal was a bare yes/no and swallowed the exception into a status
/// line. Splitting on "when is the write consent even asked for" is the half of F2 the user sees,
/// and it had two answers.
/// </para>
/// </summary>
internal static class PlanExecution
{
    /// <param name="executor">Holds the validated plan on <c>Trajectory.LastGeneratedYaml</c>.</param>
    /// <param name="opts">Only <see cref="AgentOptions.Apply"/> is read — it decides whether the
    /// launch already consented to a real write.</param>
    /// <param name="present">Shows what is about to run. Receives the plan YAML.</param>
    /// <param name="confirm">
    /// Asks for the go-ahead. Called <b>only</b> when <see cref="AgentOptions.Apply"/> is set — a
    /// dry-run is not confirmed, and asking for nothing teaches the reflex "yes". Receives the YAML.
    /// </param>
    /// <returns>
    /// The tool's result, or <c>null</c> when there was no validated plan to run or the user
    /// declined the write.
    /// </returns>
    public static async Task<ToolResult?> RunAsync(
        AgentExecutor executor,
        AgentOptions opts,
        Func<string, Task> present,
        Func<string, Task<bool>> confirm,
        CancellationToken ct)
    {
        var yaml = executor.Trajectory.LastGeneratedYaml;
        if (string.IsNullOrWhiteSpace(yaml))
            return null;

        await present(yaml);

        if (opts.Apply && !await confirm(yaml))
            return null;

        try
        {
            return await executor.ExecuteValidatedPlanAsync(ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // The tool threw instead of returning a failure result. Both surfaces render a
            // ToolResult; hand them one rather than let the exception tear the turn down.
            return ToolResult.Error(JsonSerializer.Serialize(new { error = ex.Message }));
        }
    }
}
