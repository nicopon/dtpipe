using System.CommandLine;
using DtPipe.Sessions;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

/// <summary>
/// Inspects and removes the session stores that hold materialised artefacts.
///
/// The family the CLI already has (secret, mcp, inspect) extended by one, rather than a
/// second convention. <c>export</c> is the deliberate way out of the store: encryption is not
/// optional, so the answer to "I want these rows in a real file" is a destination and the
/// ordinary writer path — not a flag that would void the store's guarantee for every session
/// in it.
/// </summary>
public class SessionCommand : Command
{
    private readonly IAnsiConsole _console;

    public SessionCommand(IAnsiConsole console) : base("session", "Inspect and purge the local session stores")
    {
        _console = console;
        Subcommands.Add(CreateListCommand());
        Subcommands.Add(CreateShowCommand());
        Subcommands.Add(CreatePurgeCommand());
    }

    private Command CreateListCommand()
    {
        var cmd = new Command("list", "List the sessions in the current store");
        cmd.SetAction(_ =>
        {
            var identity = SessionResolver.Resolve();
            if (!Directory.Exists(identity.RootPath))
            {
                _console.MarkupLine($"[grey]No session store here ({Markup.Escape(identity.RootPath)}).[/]");
                return;
            }

            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Session");
            table.AddColumn("Checkpoints");
            table.AddColumn("Size");
            table.AddColumn("Last used");
            table.AddColumn("Expires in");

            var any = false;
            foreach (var path in SessionStore.EnumerateSessionPaths(identity.RootPath).OrderBy(p => p, StringComparer.Ordinal))
            {
                any = true;
                var name = Path.GetFileName(path)!;
                var store = new SessionStore(new SessionIdentity(name, identity.RootPath, identity.Origin));
                var meta = store.ReadMetadata();
                var checkpoints = new CheckpointStore(store).List().Count;

                var lastUsed = meta?.LastUsedAt is { } lu && DateTime.TryParse(lu, out var parsed) ? parsed : Directory.GetLastWriteTimeUtc(path);
                var ttl = meta?.TtlDays ?? SessionStore.DefaultTtlDays;
                var remaining = lastUsed.AddDays(ttl) - DateTime.UtcNow;

                table.AddRow(
                    Markup.Escape(name),
                    checkpoints.ToString(),
                    FormatSize(DirectorySize(path)),
                    lastUsed.ToLocalTime().ToString("yyyy-MM-dd HH:mm"),
                    remaining > TimeSpan.Zero ? $"{(int)remaining.TotalDays}d" : "[yellow]expired[/]");
            }

            if (!any)
            {
                _console.MarkupLine($"[grey]No sessions in {Markup.Escape(identity.RootPath)}.[/]");
                return;
            }

            _console.Write(table);
            _console.MarkupLine($"[grey]Store: {Markup.Escape(identity.RootPath)}[/]");
        });
        return cmd;
    }

    private Command CreateShowCommand()
    {
        var cmd = new Command("show", "Show one session's checkpoints");
        var nameArg = new Argument<string>("name") { Description = "Session name" };
        cmd.Arguments.Add(nameArg);

        cmd.SetAction(parseResult =>
        {
            var name = parseResult.GetValue(nameArg);
            if (string.IsNullOrEmpty(name)) return;

            var identity = SessionResolver.Resolve(name);
            var store = new SessionStore(new SessionIdentity(name, identity.RootPath, identity.Origin));
            if (!store.Exists)
            {
                _console.MarkupLine($"[red]No session '{Markup.Escape(name)}' in {Markup.Escape(identity.RootPath)}.[/]");
                return;
            }

            var checkpoints = new CheckpointStore(store).List();
            if (checkpoints.Count == 0)
            {
                _console.MarkupLine($"[grey]Session '{Markup.Escape(name)}' holds no checkpoints.[/]");
                return;
            }

            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Checkpoint");
            table.AddColumn("Size");
            table.AddColumn("Written");

            foreach (var key in checkpoints)
            {
                var path = new CheckpointStore(store).PathFor(key);
                var info = new FileInfo(path);
                table.AddRow(Markup.Escape(key), FormatSize(info.Length), info.LastWriteTime.ToString("yyyy-MM-dd HH:mm"));
            }

            _console.Write(table);
            _console.MarkupLine($"[grey]A checkpoint is named by what produces it, not by an alias — see --checkpoint.[/]");
        });
        return cmd;
    }

    private Command CreatePurgeCommand()
    {
        var cmd = new Command("purge", "Remove a session, or every expired one");
        var nameArg = new Argument<string?>("name") { Description = "Session to remove; omit to purge only expired sessions", Arity = ArgumentArity.ZeroOrOne };
        cmd.Arguments.Add(nameArg);

        cmd.SetAction(parseResult =>
        {
            var name = parseResult.GetValue(nameArg);
            var identity = SessionResolver.Resolve(name);

            if (string.IsNullOrEmpty(name))
            {
                var removed = SessionPurge.PurgeExpired(identity.RootPath);
                _console.MarkupLine(removed.Count == 0
                    ? "[grey]Nothing expired.[/]"
                    : $"[green]Purged {removed.Count} expired session(s): {Markup.Escape(string.Join(", ", removed))}[/]");
                return;
            }

            var store = new SessionStore(new SessionIdentity(name, identity.RootPath, identity.Origin));
            if (!store.Exists)
            {
                _console.MarkupLine($"[red]No session '{Markup.Escape(name)}' in {Markup.Escape(identity.RootPath)}.[/]");
                return;
            }

            SessionPurge.Remove(name, store.SessionPath);
            _console.MarkupLine($"[green]Purged session '{Markup.Escape(name)}' and destroyed its key.[/]");
        });
        return cmd;
    }

    private static long DirectorySize(string path)
    {
        try
        {
            return Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
        }
        catch
        {
            return 0;
        }
    }

    private static string FormatSize(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:F1} KB",
        < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):F1} MB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):F2} GB"
    };
}
