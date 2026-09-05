using System;
using System.IO;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Writing the validated plan's YAML to disk. One copy, because both surfaces offer it — the
/// scrollback menu and the full-screen session's <c>/save</c> — and each words the outcome its
/// own way.
/// </summary>
internal static class PlanFile
{
    /// <summary>What the save offers when the user names no path.</summary>
    public const string DefaultPath = "pipeline.yaml";

    /// <summary>Writes <paramref name="yaml"/> to <paramref name="path"/>; returns null on
    /// success, or the failure message.</summary>
    public static string? TrySave(string path, string yaml)
    {
        try
        {
            File.WriteAllText(path, yaml);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }
}
