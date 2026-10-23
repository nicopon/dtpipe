using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// A command line as the job file it stands for, through the parsing and conversion a run applies:
/// what <c>--export-job</c> writes. A <c>${{NAME}}</c> or <c>keyring://</c> in it stays as typed, whether it came from the
/// command line or from a <c>--job</c> file: the text is read as written, and nothing is resolved before it is used.
/// </summary>
public static class CommandLineJob
{
    /// <summary>
    /// Why a command line with a binding cannot be exported, or null when it holds none. A binding names a
    /// location meaningful to one run, which a job file meant for git must not carry.
    /// </summary>
    public static string? BindingRefusal(GlobalOptions globals) =>
        string.IsNullOrEmpty(globals.BindInput) && string.IsNullOrEmpty(globals.BindOutput)
            ? null
            : "--export-job cannot be combined with --bind-input/--bind-output: "
              + "a binding names a run-specific location, and exporting it would write that path into a file meant for git.";

    /// <summary>The job file text of <paramref name="args"/>; throws <see cref="ArgumentException"/> when they cannot be exported.</summary>
    public static string Export(IServiceProvider services, IReadOnlyList<string> args)
    {
        var registry = services.GetRequiredService<FlagRegistry>();
        var parsed = new PipelineLexer(registry).Parse([.. args]);
        if (BindingRefusal(parsed.Globals) is { } refusal) throw new ArgumentException(refusal);
        var (jobs, _, _) = PipelineToJobConverter.Convert(
            parsed,
            services.GetRequiredService<IEnumerable<IStreamTransformerFactory>>(),
            services.GetRequiredService<DtPipe.Cli.Security.ISecretsManager>(),
            services.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
            services.GetRequiredService<IEnumerable<IDataWriterFactory>>(),
            services.GetRequiredService<IEnumerable<IDataTransformerFactory>>(),
            registry,
            interpolate: false);
        return DtPipe.Configuration.JobFileWriter.Serialize(jobs);
    }
}
