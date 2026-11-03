using System.CommandLine;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Cli.Commands;

/// <summary>
/// <c>dtpipe keygen &lt;id&gt; [--out file]</c>: a key for the identity <c>id</c>, in the shape a coordinator reads
/// (<see cref="CoordinatorApi.NewKey"/>). It reaches no coordinator and keeps nothing: the key is the only output, on
/// standard output or in a file the command creates and never replaces. The person gives it to the coordinator
/// (<c>Coordinator:Bootstrap</c>) and to the host it opens, as a file reference.
/// </summary>
public sealed class KeygenCommand : Command
{
    public KeygenCommand() : base("keygen", "Make the key of an identity of a coordinator: a new random key on standard output")
    {
        var id = new Argument<string>("id") { Description = "The client id of the identity the key opens: lowercase letters, digits and '-'" };
        var output = new Option<string?>("--out")
        {
            Description = "Write the key to this file (readable by its owner only) instead of standard output; an existing file is never replaced",
        };
        Arguments.Add(id);
        Options.Add(output);
        this.SetAction(parseResult => Make(parseResult.GetValue(id)!, parseResult.GetValue(output)));
    }

    private static int Make(string clientId, string? path)
    {
        if (!CoordinatorApi.IsPipelineId(clientId))
        {
            Console.Error.WriteLine($"[ERROR] '{clientId}' is not a client id: lowercase letters, digits and '-', a letter or digit first, 63 characters at most.");
            return 1;
        }

        var key = CoordinatorApi.NewKey(clientId);
        if (path is null)
        {
            Console.Out.WriteLine(key);
            return 0;
        }

        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using var file = new StreamWriter(path, options);
            file.Write(key);
        }
        catch (IOException) when (File.Exists(path))
        {
            Console.Error.WriteLine($"[ERROR] '{path}' exists: a key is never replaced. Remove the file first if you mean to make another.");
            return 1;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Console.Error.WriteLine($"[ERROR] '{path}' cannot be written: {ex.Message}");
            return 1;
        }

        Console.Error.WriteLine($"Key for '{clientId}' written to {path}.");
        return 0;
    }
}
