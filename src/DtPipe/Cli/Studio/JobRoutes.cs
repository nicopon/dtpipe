using System.Text.Json.Nodes;
using DtPipe.Cli.Expressions;
using DtPipe.Jobs;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Workspace job, design, variable, launch override, view preference, component palette, and CLI import endpoints.
/// </summary>
internal sealed class JobRoutes(
    WorkspaceSlot workspace,
    CoordinatorSlot coordinator,
    StudioTools tools,
    StudioDesignContext design,
    DebugRoutes debug,
    RunRoutes runs)
{
    private static readonly string[] PaneNames = ["pal", "insp", "drawer", "agent"];
    private static readonly string[] ViewTabs = ["yaml", "preview", "variables"];
    private static readonly string[] ViewSides = ["jobs", "library", "steps", "none"];

    private sealed record DecomposeRequest(string? Yaml, JsonObject? Layout);

    private sealed record ViewJobRequest(string? Tab, int? Rows, string? Card, bool? Review = null);

    private sealed record ViewRequest(Dictionary<string, double>? Sizes, string? Path, ViewJobRequest? Job, string? Open = null, string? Side = null);

    private sealed record SaveRequest(string? Path, string? Hash, DesignModel? Model, bool AcceptErrors);

    private sealed record RenameJobRequest(string? From, string? To, string? Hash);

    private sealed record ImportRequest(List<string?>? Args);

    private sealed record VariablesRequest(string? Path, string? Yaml, List<SampledBrick>? Bricks);

    private sealed record LaunchRequest(string? Path, Dictionary<string, StudioOverride?>? Overrides);

    private StudioWorkspace CurrentWorkspace => workspace.Current;

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/jobs", JobsAsync);
        routes.Map("GET", "/api/job", context => OpenAsync(context, context.Request.Query["path"].ToString()));
        routes.Map("PUT", "/api/job", SaveAsync);
        routes.Map("POST", "/api/job/rename", RenameJobAsync);
        routes.Map("DELETE", "/api/job", DeleteJobAsync);
        routes.Map("GET", "/api/components", context => StudioHttp.JsonAsync(context, new { readers = tools.Readers, writers = tools.Writers }), RouteLocks.None);
        routes.Map("POST", "/api/design/compose", ComposeAsync);
        routes.Map("POST", "/api/design/decompose", DecomposeAsync);
        routes.Map("POST", "/api/variables", VariablesAsync);
        routes.Map("PUT", "/api/launch", LaunchAsync);
        routes.Map("GET", "/api/view", GetViewAsync);
        routes.Map("PUT", "/api/view", ViewAsync);
        routes.Map("POST", "/api/import", ImportAsync);
    }

    private async Task ComposeAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, design.WithRunHere(design.Design().Compose(await StudioHttp.BodyAsync<DesignModel>(context))));

    private async Task DecomposeAsync(HttpContext context)
    {
        var decompose = await StudioHttp.BodyAsync<DecomposeRequest>(context);
        await StudioHttp.JsonAsync(context, (await design.DesignForAsync(decompose.Yaml ?? "", context.RequestAborted)).Decompose(decompose.Yaml ?? "", decompose.Layout));
    }

    private async Task VariablesAsync(HttpContext context)
    {
        var asked = await StudioHttp.BodyAsync<VariablesRequest>(context);
        // A brick's branch runs on its node, which resolves its variables: they are none of this machine's.
        var variables = CurrentWorkspace.Variables.Of(design.JobPath(asked.Path), design.WithoutBricks(asked.Yaml ?? "", asked.Bricks));
        // The keyring's aliases are listed only for a job with a variable to launch.
        var aliases = variables.Any(v => v.Kind == "env") ? CurrentWorkspace.Variables.Aliases() : [];
        await StudioHttp.JsonAsync(context, new { variables, aliases });
    }

    private async Task GetViewAsync(HttpContext context)
    {
        var viewed = CurrentWorkspace.Local.ReadView();
        var viewedJob = design.JobPath(context.Request.Query["path"].ToString());
        await StudioHttp.JsonAsync(context, new { sizes = viewed["sizes"], side = viewed["side"], open = viewed["open"], job = viewedJob is null ? null : viewed["jobs"]?[viewedJob] });
    }

    /// <summary>
    /// Lists the jobs and subdirectories of the folder, along with each job's link to the active coordinator's library.
    /// </summary>
    private async Task JobsAsync(HttpContext context)
    {
        var ws = CurrentWorkspace;
        var jobs = ws.Folder.Jobs();
        var folders = ws.Folder.Folders();
        var publications = new Dictionary<string, object>(StringComparer.Ordinal);
        if (coordinator.Current is { } link)
        {
            var origin = link.Origin.ToString();
            foreach (var job in jobs)
            {
                StudioPublication? pub;
                try { pub = ws.Local.ReadPublication(job); }
                catch (Exception ex) when (ex is StudioRefusal or IOException or UnauthorizedAccessException) { continue; }
                if (pub is null || pub.Coordinator != origin) continue;
                bool? localModified = null;
                if (pub.LocalHash is not null)
                {
                    try { localModified = ws.Folder.ReadJob(job).Hash != pub.LocalHash; }
                    catch (Exception ex) when (ex is StudioRefusal or IOException or UnauthorizedAccessException) { }
                }
                publications[job] = new { id = pub.Id, commit = pub.Commit, localModified };
            }
        }
        await StudioHttp.JsonAsync(context, new { folder = Path.GetFileName(ws.Folder.Root), jobs, folders, publications });
    }

    /// <summary>
    /// Moves a job and its local studio metadata inside the folder, refusing collisions, active/suspended runs, and stale hashes.
    /// </summary>
    private async Task RenameJobAsync(HttpContext context)
    {
        var asked = await StudioHttp.BodyAsync<RenameJobRequest>(context);
        var from = design.JobPath(asked.From) ?? throw new StudioRefusal(400, "Name the job's current path as 'from'.");
        var to = design.JobPath(asked.To) ?? throw new StudioRefusal(400, "Name the job's new path as 'to'.");
        var ws = CurrentWorkspace;
        ws.Local.CheckJobFiles(from);
        ws.Local.CheckJobFiles(to);
        if (runs.RunInProgressFor(from))
            throw new StudioRefusal(409, $"A run is in progress on '{from}': stop it, or wait for it, before renaming.");
        if (debug.LoadDebug(from).Real)
            throw new StudioRefusal(409, $"A real run is suspended on '{from}': write or abandon it before renaming.");
        var hash = ws.Folder.RenameJob(from, to, asked.Hash);
        ws.Local.MoveJobFiles(from, to);
        await StudioHttp.JsonAsync(context, new { path = to, hash });
    }

    /// <summary>
    /// Deletes a job and its local studio metadata and checkpoints, refusing active/suspended runs and stale hashes.
    /// </summary>
    private Task DeleteJobAsync(HttpContext context)
    {
        var relative = design.JobPath(context.Request.Query["path"].ToString())
            ?? throw new StudioRefusal(400, "Name the job's path to delete.");
        var hashParam = context.Request.Query["hash"].ToString();
        var expectedHash = string.IsNullOrEmpty(hashParam) ? null : hashParam;
        var ws = CurrentWorkspace;
        ws.Local.CheckJobFiles(relative);
        if (runs.RunInProgressFor(relative))
            throw new StudioRefusal(409, $"A run is in progress on '{relative}': stop it, or wait for it, before deleting.");
        if (debug.LoadDebug(relative).Real)
            throw new StudioRefusal(409, $"A real run is suspended on '{relative}': write or abandon it before deleting.");
        ws.Folder.DeleteJob(relative, expectedHash);
        ws.Local.DeleteJobFiles(relative, key => DebugRoutes.ForgetCheckpoint(debug.DebugStore(), key));
        context.Response.StatusCode = StatusCodes.Status204NoContent;
        return Task.CompletedTask;
    }

    /// <summary>
    /// A job, the hash a save must find again, its layout, and whether saving would change its text:
    /// the server writes what it composes, so a file written by hand loses its comments and its order.
    /// </summary>
    private async Task OpenAsync(HttpContext context, string relative)
    {
        var ws = CurrentWorkspace;
        var job = ws.Folder.ReadJob(relative);
        var rewrites = (await design.DesignForAsync(job.Text, context.RequestAborted)).Decompose(job.Text).Yaml != job.Text.Replace("\r\n", "\n");
        await StudioHttp.JsonAsync(context, new { path = relative, yaml = job.Text, hash = job.Hash, layout = ws.Local.ReadLayout(relative), rewrites });
    }

    /// <summary>
    /// Writes the job the server composes from the page's cards, never a text the page sends, then
    /// the cards' layout beside the folder's files. A job whose cards do not compose is written only
    /// when the page says the person accepted it.
    /// </summary>
    private async Task SaveAsync(HttpContext context)
    {
        var save = await StudioHttp.BodyAsync<SaveRequest>(context);
        if (save.Model is null) throw new StudioRefusal(400, "A save carries the steps as 'model'.");
        var composed = design.Design().Compose(save.Model);
        if (!composed.Valid && !save.AcceptErrors)
            throw new StudioRefusal(422, "The steps do not compose into a job: " + string.Join(" ", composed.Errors));
        var ws = CurrentWorkspace;
        ws.Folder.ResolveForWrite(save.Path ?? "");
        if (save.Model.Layout is not null) ws.Local.CheckLayout(save.Path!);
        var hash = ws.Folder.WriteJob(save.Path!, composed.Yaml, save.Hash);
        if (save.Model.Layout is { } layout) ws.Local.WriteLayout(save.Path!, layout);
        await StudioHttp.JsonAsync(context, new { path = save.Path, hash, yaml = composed.Yaml });
    }

    /// <summary>
    /// Keeps what each variable of a job is launched with: a keyring alias or a typed text. A value
    /// from the keyring is never stored, only its alias.
    /// </summary>
    private async Task LaunchAsync(HttpContext context)
    {
        var launch = await StudioHttp.BodyAsync<LaunchRequest>(context);
        var job = design.JobPath(launch.Path) ?? throw new StudioRefusal(400, "Launch values belong to a job: name its path.");
        var overrides = new Dictionary<string, StudioOverride>(StringComparer.Ordinal);
        foreach (var (name, chosen) in launch.Overrides ?? [])
        {
            if (chosen is null || chosen.Keyring is null == chosen.Value is null)
                throw new StudioRefusal(400, $"'{name}' is launched with either a keyring alias or a text.");
            if (chosen.Value is { Length: > 65_536 })
                throw new StudioRefusal(413, $"The text for '{name}' is too long.");
            overrides[name] = chosen;
        }
        CurrentWorkspace.Local.WriteLaunch(job, overrides);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    /// <summary>
    /// Keeps how the page was left: the sizes of its panes, the side panel chosen, the job open and, for a job, its tab, the rows
    /// asked and the selected card.
    /// Each value is checked and rewritten, so the file holds nothing the page did not mean as a preference.
    /// </summary>
    private async Task ViewAsync(HttpContext context)
    {
        var view = await StudioHttp.BodyAsync<ViewRequest>(context);
        JsonObject? sizes = null;
        if (view.Sizes is not null)
        {
            sizes = [];
            foreach (var (pane, size) in view.Sizes)
            {
                if (!PaneNames.Contains(pane) || !(size is >= 0 and <= 5000)) throw new StudioRefusal(400, $"'{pane}' is no pane, or its size is out of range.");
                sizes[pane] = (int)size;
            }
        }

        if (view.Side is not null && !ViewSides.Contains(view.Side)) throw new StudioRefusal(400, $"'{view.Side}' is no side panel.");

        JsonObject? jobView = null;
        var job = design.JobPath(view.Path);
        if (job is not null && view.Job is { } kept)
        {
            if (kept.Tab is not null && !ViewTabs.Contains(kept.Tab)) throw new StudioRefusal(400, $"'{kept.Tab}' is no tab to keep.");
            if (kept.Rows is not null && !(kept.Rows is >= 1 and <= StudioServer.DebugMaxRows)) throw new StudioRefusal(400, "The rows asked are out of range.");
            if (kept.Card is { Length: > 200 }) throw new StudioRefusal(400, "The step's name is too long.");
            jobView = [];
            if (kept.Tab is not null) jobView["tab"] = kept.Tab;
            if (kept.Rows is not null) jobView["rows"] = kept.Rows;
            if (kept.Card is not null) jobView["card"] = kept.Card;
            if (kept.Review is not null) jobView["review"] = kept.Review;
        }

        // The job open is a path of the folder like any other the page names: the one the studio opens again at its start.
        CurrentWorkspace.Local.UpdateView(sizes, job, jobView, design.JobPath(view.Open), view.Side);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    /// <summary>
    /// The job a command line stands for, as <c>--export-job</c> would write it: the arguments arrive as the
    /// array the page split, never as one string to cut here. The result is not saved anywhere.
    /// </summary>
    private async Task ImportAsync(HttpContext context)
    {
        var import = tools.Import ?? throw new StudioRefusal(404, "This studio imports no command line.");
        var asked = await StudioHttp.BodyAsync<ImportRequest>(context);
        if (asked.Args is not { Count: > 0 } args || args.Any(a => a is null))
            throw new StudioRefusal(400, "An import carries the arguments of a command as 'args'.");
        if (args.Sum(a => a!.Length) > 65_536) throw new StudioRefusal(413, "The command is too long.");
        string yaml;
        try
        {
            using var _ = NonInteractiveGuard.Suppress();
            yaml = import(args!);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new StudioRefusal(422, $"This command does not make a job: {StudioHttp.Shown(ex)}");
        }
        await StudioHttp.JsonAsync(context, new { yaml });
    }
}
