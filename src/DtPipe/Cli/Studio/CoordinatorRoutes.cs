using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Coordinator-backed routes: sign-in/out, OIDC callback, brick catalog and preview, library browsing and pull,
/// job publication, publication dry-run check, and live edge protections.
/// </summary>
internal sealed class CoordinatorRoutes(
    WorkspaceSlot workspace,
    CoordinatorSlot coordinator,
    StudioDesignContext design,
    Func<int> getPort)
{
    private sealed record OpenLibraryRequest(string? Id, string? Path = null, string? OverwriteHash = null);

    private sealed record BrickPreviewRequest(string? Key);

    private sealed record PublishRequest(string? Path, string? Hash, string? Id, string? Message, string? Replace);

    private sealed record PublicationCheckRequest(string? Path, string? Hash, string? Yaml);

    private StudioWorkspace CurrentWorkspace => workspace.Current;

    private CoordinatorLink? Coordinator => coordinator.Current;

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/bricks", BricksAsync, RouteLocks.Both);
        routes.Map("POST", "/api/bricks/preview", BrickPreviewAsync, RouteLocks.Both);
        routes.Map("GET", "/api/coordinator", context => StudioHttp.JsonAsync(context, (object?)Coordinator?.Status() ?? new { url = (string?)null }), RouteLocks.None);
        routes.Map("POST", "/api/coordinator/login", LoginAsync, RouteLocks.Both);
        routes.Map("POST", "/api/coordinator/logout", context =>
        {
            Coordinator?.SignOutNow();
            context.Response.StatusCode = StatusCodes.Status204NoContent;
            return Task.CompletedTask;
        }, RouteLocks.Workspace);
        routes.Map("GET", "/api/publish", GetPublishAsync, RouteLocks.Both);
        routes.Map("POST", "/api/publish", PublishAsync, RouteLocks.Both);
        routes.Map("POST", "/api/publish/check", PublicationCheckAsync, RouteLocks.Both);
        routes.Map("POST", "/api/publish/edges", PublicationEdgesAsync, RouteLocks.Both);
        routes.Map("GET", "/api/library", LibraryAsync, RouteLocks.Both);
        routes.Map("GET", "/api/library/job", LibraryJobAsync, RouteLocks.Both);
        routes.Map("POST", "/api/library/open", OpenFromLibraryAsync, RouteLocks.Both);
    }

    /// <summary>
    /// The identity provider sends the browser back here with a code: a cross-site navigation, so no
    /// token and no same-origin check, only the host's. What makes it this studio's is the single-use
    /// <c>state</c> the link checks before exchanging the code. The answer is a line of text, and never
    /// shows a token.
    /// </summary>
    public async Task CallbackAsync(HttpContext context, Action<HttpRequest> admitHost)
    {
        admitHost(context.Request);
        if (context.Request.Method != "GET") throw new StudioRefusal(405, "The sign-in comes back as a GET.");
        using var hold = coordinator.Hold();
        string message;
        if (hold.Value is not { } link)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            message = StudioHttp.NoCoordinator;
        }
        else
        {
            var query = context.Request.Query;
            string? Value(string name) => query[name] is { Count: 1 } v ? v.ToString() : null;
            try
            {
                var who = await link.CompleteSignInAsync(Value("code"), Value("state"), Value("iss"), Value("error"), context.RequestAborted);
                message = $"Signed in to {link.Origin.ToString().TrimEnd('/')} as {who}. You can close this tab and go back to dtpipe studio.";
            }
            catch (CoordinatorLinkException ex)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                message = ex.Message;
            }
        }
        context.Response.ContentType = "text/plain; charset=utf-8";
        await context.Response.WriteAsync(message + "\n", context.RequestAborted);
    }

    private async Task LoginAsync(HttpContext context)
    {
        var link = Coordinator ?? throw new StudioRefusal(404, StudioHttp.NoCoordinator);
        // With a key there is no tab to open: the studio signs in here, and the page reads the status again.
        try
        {
            if (link.ByKey)
            {
                await link.SignInWithKeyAsync(context.RequestAborted);
                await StudioHttp.JsonAsync(context, new { url = (string?)null });
            }
            else await StudioHttp.JsonAsync(context, new { url = await link.BeginSignInAsync(getPort(), context.RequestAborted) });
        }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, ex.Message); }
    }

    private async Task GetPublishAsync(HttpContext context)
    {
        var published = design.JobPath(context.Request.Query["path"].ToString()) is { } publishedJob
            ? CurrentWorkspace.Local.ReadPublication(publishedJob)
            : null;
        // An id the page proposes is judged by the rule a publication applies, before the person writes a message for it.
        var proposed = context.Request.Query["id"].ToString();
        await StudioHttp.JsonAsync(context, new
        {
            publication = published?.Coordinator == Coordinator?.Origin.ToString() ? published : null,
            idError = proposed.Length == 0 || CoordinatorApi.IsPipelineId(proposed) ? null : StudioHttp.NotAPipelineId(proposed),
        });
    }

    /// <summary>
    /// Publishes the job as it is saved in the folder, never the page's cards: read again from disk and
    /// refused when it is not the text the page saved (its hash). Its launch file and its layout stay on
    /// this machine. A first publication creates only; a later one replaces only the commit published
    /// last from here, or the one the person chose to replace after a 412. It sends no binding: a reference names its node and its
    /// brick, and the coordinator keeps the mode and writes the pin.
    /// </summary>
    private async Task PublishAsync(HttpContext context)
    {
        var link = Coordinator ?? throw new StudioRefusal(404, StudioHttp.NoCoordinator);
        var asked = await StudioHttp.BodyAsync<PublishRequest>(context);
        var path = design.JobPath(asked.Path) ?? throw new StudioRefusal(400, "Publish a job saved in the folder: name its path.");
        if (!CoordinatorApi.IsPipelineId(asked.Id)) throw new StudioRefusal(400, StudioHttp.NotAPipelineId(asked.Id));
        var ws = CurrentWorkspace;
        var job = ws.Folder.ReadJob(path);
        if (asked.Hash is null || asked.Hash != job.Hash)
            throw new StudioRefusal(409, $"{path} is not the job saved last: save it, then publish.");
        if (StudioDesignContext.SecretsRefusal(job.Text) is { } secrets)
        {
            context.Response.StatusCode = StatusCodes.Status422UnprocessableEntity;
            await StudioHttp.JsonAsync(context, new { error = "the job holds a credential as a value.", report = secrets });
            return;
        }

        var origin = link.Origin.ToString();
        var last = ws.Local.ReadPublication(path) is { } p && p.Coordinator == origin && p.Id == asked.Id ? p : null;
        (LibrarySaved? Saved, LibraryConflict? Conflict, LibraryRefused? Refused) outcome;
        // Replace names the commit the person chose to replace; empty, it creates a pipeline gone since.
        var expected = asked.Replace is { } chosen ? (chosen.Length == 0 ? null : chosen) : last?.Commit;
        try { outcome = await link.PublishAsync(asked.Id!, job.Text, asked.Message, expected, context.RequestAborted); }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, ex.Message); }
        if (outcome.Conflict is { } conflict)
        {
            context.Response.StatusCode = StatusCodes.Status412PreconditionFailed;
            await StudioHttp.JsonAsync(context, new { error = conflict.Error, commit = conflict.Commit });
            return;
        }
        if (outcome.Refused is { } refused)
        {
            context.Response.StatusCode = StatusCodes.Status422UnprocessableEntity;
            await StudioHttp.JsonAsync(context, new { error = refused.Error, report = refused.Report });
            return;
        }
        var saved = outcome.Saved!;
        ws.Local.WritePublication(path, new StudioPublication(origin, saved.Id, saved.Commit, job.Hash));
        await StudioHttp.JsonAsync(context, saved);
    }

    /// <summary>
    /// What a publication of a job would refuse, judged as the coordinator's runner judges it (<see cref="PublicationCheck"/>):
    /// the job saved in the folder (<c>path</c> and the <c>hash</c> the page saved) or the cards' composition (<c>yaml</c>).
    /// Each source brick is bound to the first row its node sends through the coordinator, as a publication's check samples it,
    /// and each sink brick to a throwaway file. Only while a person is signed in: a brick's rows come through the coordinator,
    /// and a verdict without them would be another check. Asked by the page, never at each edit: it starts a process.
    /// </summary>
    private async Task PublicationCheckAsync(HttpContext context)
    {
        var link = Coordinator ?? throw new StudioRefusal(404, StudioHttp.NoCoordinator);
        if (link.Status().State != CoordinatorLink.SignedIn)
            throw new StudioRefusal(409, "Sign in to the coordinator: what a publication would refuse is judged over its bricks' rows.");
        var yaml = await JobToJudgeAsync(context);
        await StudioHttp.JsonAsync(context, await JudgeForPublicationAsync(link, yaml, context.RequestAborted));
    }

    /// <summary>The job a verdict of publication is asked of: the one saved in the folder (<c>path</c> and its <c>hash</c>) or the cards' composition (<c>yaml</c>).</summary>
    private async Task<string> JobToJudgeAsync(HttpContext context)
    {
        var asked = await StudioHttp.BodyAsync<PublicationCheckRequest>(context);
        if (asked.Path is not { } asPath)
            return asked.Yaml ?? throw new StudioRefusal(400, "A check is asked as {\"yaml\": \"...\"} or {\"path\": \"...\", \"hash\": \"...\"}.");
        var path = design.JobPath(asPath) ?? throw new StudioRefusal(400, "Check a job saved in the folder: name its path.");
        var job = CurrentWorkspace.Folder.ReadJob(path);
        if (asked.Hash is null || asked.Hash != job.Hash)
            throw new StudioRefusal(409, $"{path} is not the job saved last: save it, then check it.");
        return job.Text;
    }

    /// <summary>
    /// What the coordinator's nodes would do with the edges of a job, as the coordinator words it (<see cref="CoordinatorLink.EdgeProtectionsAsync"/>):
    /// the job saved in the folder or the cards' composition, as a publication's check is asked. A read: no process starts, nothing is
    /// kept, and the answer stops nothing, a publication included. The job goes to the coordinator as a publication would send it, so a
    /// credential written as a value is refused here first, as <see cref="StudioDesignContext.SecretsRefusal"/> words it, and the answer then says the job
    /// cannot be judged. Only while a person is signed in.
    /// </summary>
    private async Task PublicationEdgesAsync(HttpContext context)
    {
        var link = Coordinator ?? throw new StudioRefusal(404, StudioHttp.NoCoordinator);
        if (link.Status().State != CoordinatorLink.SignedIn)
            throw new StudioRefusal(409, "Sign in to the coordinator: what its nodes would do with the job's edges is read from it.");
        var yaml = await JobToJudgeAsync(context);
        if (StudioDesignContext.SecretsRefusal(yaml) is { } secrets)
        {
            await StudioHttp.JsonAsync(context, new EdgeProtectionsResponse(false, $"The job was not sent: {secrets}", []));
            return;
        }
        try { await StudioHttp.JsonAsync(context, await link.EdgeProtectionsAsync(yaml, context.RequestAborted)); }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, ex.Message); }
    }

    private async Task<PublicationVerdict> JudgeForPublicationAsync(CoordinatorLink link, string yaml, CancellationToken ct)
    {
        var catalog = link.Catalog ?? new BrickCatalogView([], []);
        IReadOnlyDictionary<string, BrickReference> references;
        try { references = BrickRef.In(JobYaml.Parse(yaml)); }
        catch (Exception ex) when (ex is FormatException or YamlDotNet.Core.YamlException) { references = new Dictionary<string, BrickReference>(); }
        // A reference is bound only to a brick of its own kind: a source's rows read as a sink's target would pass for a job the
        // runner takes. Any other is left as written, for the dry run to refuse.
        var bound = references.Values
            .Where(r => catalog.Bricks.FirstOrDefault(b => b.Key == r.Key) is { } brick && (brick.Kind == StepKinds.Sink) == r.IsSink)
            .Select(r => new SampledBrick(r.Alias, r.Key)).ToList();
        if (references.Values.Any(r => catalog.Bricks.All(b => b.Key != r.Key)))
        {
            try { StudioDesignContext.RefuseReferences(yaml, catalog); }
            catch (StudioRefusal refusal) { return new PublicationVerdict(false, refusal.Message); }
        }
        var sampling = BrickSampling.Of(yaml, bound, catalog);

        var directory = CurrentWorkspace.Local.CheckDirectory();
        try
        {
            var sources = new List<(string Alias, byte[] Arrow)>();
            foreach (var (alias, brick) in sampling.Sources)
            {
                // One row, as a publication's check asks each source brick's node.
                try { sources.Add((alias, await link.SampleAsync(brick.Node, brick.Id, 1, ct))); }
                catch (CoordinatorLinkException ex) { return new PublicationVerdict(false, $"'{alias}': {ex.Message}"); }
            }
            return await PublicationCheck.RunAsync(directory, sampling.Yaml, sources, sampling.Sinks, ct);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch (IOException) { }
        }
    }

    /// <summary>The coordinator's link when a person is signed in to it; a refusal that names what is missing otherwise.</summary>
    private CoordinatorLink SignedInLink() =>
        Coordinator is not { } link ? throw new StudioRefusal(404, StudioHttp.NoCoordinator)
        : link.Status().State != CoordinatorLink.SignedIn ? throw new StudioRefusal(409, "Sign in to the coordinator to read its library.")
        : link;

    private async Task LibraryAsync(HttpContext context)
    {
        var link = SignedInLink();
        try { await StudioHttp.JsonAsync(context, new { pipelines = await link.LibraryAsync(context.RequestAborted) }); }
        catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException) { throw new StudioRefusal(502, ex.Message); }
    }

    /// <summary>Reads a library pipeline's YAML and commit without writing anything to disk.</summary>
    private async Task LibraryJobAsync(HttpContext context)
    {
        var link = SignedInLink();
        var id = context.Request.Query["id"].ToString();
        if (!CoordinatorApi.IsPipelineId(id))
            throw new StudioRefusal(400, StudioHttp.NotAPipelineId(id));
        LibraryJob job;
        try
        {
            job = await link.LibraryJobAsync(id, context.RequestAborted)
                ?? throw new StudioRefusal(404, $"'{id}' is not in the library.");
        }
        catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException)
        {
            throw new StudioRefusal(502, ex.Message);
        }
        await StudioHttp.JsonAsync(context, new { id = job.Id, yaml = job.Yaml, commit = job.Commit });
    }

    /// <summary>
    /// Opens a library pipeline in the folder (create-only by default, or overwriting the file matching
    /// <c>overwriteHash</c>), keeping the commit and local file hash for sync and later publications.
    /// </summary>
    private async Task OpenFromLibraryAsync(HttpContext context)
    {
        var link = SignedInLink();
        var asked = await StudioHttp.BodyAsync<OpenLibraryRequest>(context);
        if (!CoordinatorApi.IsPipelineId(asked.Id))
            throw new StudioRefusal(400, $"'{asked.Id}' is not a pipeline id: lowercase letters, digits and '-'.");
        var path = string.IsNullOrWhiteSpace(asked.Path)
            ? asked.Id + ".yaml"
            : design.JobPath(asked.Path)!;
        LibraryJob job;
        try { job = await link.LibraryJobAsync(asked.Id!, context.RequestAborted) ?? throw new StudioRefusal(404, $"'{asked.Id}' is not in the library."); }
        catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException) { throw new StudioRefusal(502, ex.Message); }
        var ws = CurrentWorkspace;
        ws.Local.CheckPublication(path);
        var expectedHash = string.IsNullOrEmpty(asked.OverwriteHash) ? null : asked.OverwriteHash;
        var hash = ws.Folder.WriteJob(path, job.Yaml, expectedHash);
        ws.Local.WritePublication(path, new StudioPublication(link.Origin.ToString(), job.Id, job.Commit, hash));
        await StudioHttp.JsonAsync(context, new { path, hash, commit = job.Commit });
    }

    /// <summary>
    /// The coordinator's catalog, read again when a person is signed in, with this dtpipe's version: the
    /// palette's bricks and the versions the page compares. The catalog read last when it cannot be read.
    /// </summary>
    private async Task BricksAsync(HttpContext context)
    {
        var link = Coordinator;
        string? error = null;
        BrickCatalogView? catalog = null;
        if (link is not null)
        {
            try { catalog = await link.RefreshCatalogAsync(context.RequestAborted); }
            catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException)
            {
                error = ex.Message;
                catalog = link.Catalog;
            }
        }
        await StudioHttp.JsonAsync(context, new
        {
            studioVersion = CoordinatorLink.StudioVersion,
            nodes = catalog?.Nodes ?? [],
            bricks = catalog?.Bricks ?? [],
            error,
        });
    }

    /// <summary>
    /// A brick's first rows and their types, read by its node when a person looks at its card: the catalog carries no
    /// schema, which depends on the rows. Read each time, kept nowhere, and outside the engine's slot since nothing runs here.
    /// </summary>
    private async Task BrickPreviewAsync(HttpContext context)
    {
        var link = Coordinator ?? throw new StudioRefusal(404, StudioHttp.NoCoordinator);
        var asked = await StudioHttp.BodyAsync<BrickPreviewRequest>(context);
        var brick = link.Catalog?.Bricks.FirstOrDefault(b => b.Key == asked.Key)
            ?? throw new StudioRefusal(409, $"'{asked.Key}' is no brick of the catalog read last: sign in to the coordinator.");
        if (link.Status().State != CoordinatorLink.SignedIn) throw new StudioRefusal(409, "Sign in to the coordinator to read the rows of a brick.");
        try { await StudioHttp.JsonAsync(context, await link.PreviewAsync(brick.Node, brick.Id, context.RequestAborted)); }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, ex.Message); }
    }
}
