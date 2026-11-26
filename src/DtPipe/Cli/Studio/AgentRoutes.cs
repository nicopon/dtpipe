using System.Text.Json;
using DtPipe.Jobs;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The studio's agent: its conversation routes, and the gestures its tools make through the studio's own paths. A save is the page's
/// save (<c>Decompose</c>, <c>Compose</c>, <c>WriteJob</c> against the hash read when the card was made), which waits for the
/// person's answer to a card; no tool writes a target. With no agent connected, <c>GET /api/agent</c> answers
/// <c>enabled: false</c> and the conversation's other routes 404.
/// </summary>
internal sealed class AgentRoutes(
    WorkspaceSlot workspace,
    AgentSlot agentSlot,
    EngineSlot engineSlot,
    StudioDesignContext design,
    Func<bool> runInProgress)
{
    private sealed record AgentJob(string? Path, bool Unsaved, DesignModel? Model);

    private sealed record AgentTurnRequest(string? Prompt, string? Model, AgentJob? Job);

    private sealed record AgentAnswerRequest(string? Id, string? Choice, string? Digest);

    private sealed record AgentFeedbackRequest(int Turn, string? Rating, string? Text);

    private StudioWorkspace CurrentWorkspace => workspace.Current;

    private StudioAgent? Agent => agentSlot.Current;

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/agent", GetAgentAsync, RouteLocks.None);
        routes.Map("GET", "/api/agent/models", async context =>
            await StudioHttp.JsonAsync(context, await RequireAgent().ModelsAsync(context.RequestAborted)));
        routes.Map("POST", "/api/agent/turn", StartTurnAsync);
        routes.Map("POST", "/api/agent/stop", context =>
        {
            RequireAgent().Stop();
            context.Response.StatusCode = StatusCodes.Status202Accepted;
            return Task.CompletedTask;
        });
        routes.Map("POST", "/api/agent/answer", AnswerAsync);
        routes.Map("POST", "/api/agent/feedback", FeedbackAsync);
        routes.Map("DELETE", "/api/agent", context =>
        {
            RequireAgent().Reset();
            context.Response.StatusCode = StatusCodes.Status204NoContent;
            return Task.CompletedTask;
        });
    }

    private StudioAgent RequireAgent() =>
        Agent ?? throw new StudioRefusal(404, "No model is connected to this studio's agent.");

    private Task GetAgentAsync(HttpContext context)
    {
        if (Agent is not { } agent)
            return StudioHttp.JsonAsync(context, new { enabled = false });
        var after = int.TryParse(context.Request.Query["after"], out var a) ? a : 0;
        var rev = int.TryParse(context.Request.Query["rev"], out var r) ? r : 0;
        return StudioHttp.JsonAsync(context, agent.Status(after, rev));
    }

    private async Task StartTurnAsync(HttpContext context)
    {
        var agent = RequireAgent();
        var turn = await StudioHttp.BodyAsync<AgentTurnRequest>(context);
        if (string.IsNullOrWhiteSpace(turn.Prompt)) throw new StudioRefusal(400, "A turn has a prompt.");
        if (runInProgress()) throw new StudioRefusal(409, "A run is in progress: wait for it, or stop it, before asking the agent.");
        agent.StartTurn(turn.Prompt, turn.Model, OpenJobOf(turn.Job));
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        await StudioHttp.JsonAsync(context, new { started = true });
    }

    private async Task AnswerAsync(HttpContext context)
    {
        var agent = RequireAgent();
        var answer = await StudioHttp.BodyAsync<AgentAnswerRequest>(context);
        agent.Answer(answer.Id, answer.Choice, answer.Digest);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    private async Task FeedbackAsync(HttpContext context)
    {
        var agent = RequireAgent();
        var feedback = await StudioHttp.BodyAsync<AgentFeedbackRequest>(context);
        agent.Feedback(feedback.Turn, feedback.Rating, feedback.Text);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    /// <summary>Waits for the engine slot, as long as the turn is not stopped; the lease releases it.</summary>
    public async Task<IDisposable> TakeSlotAsync(string who, CancellationToken ct)
    {
        using var linked = Linked(ct);
        return await engineSlot.WaitAsync(who, linked.Token);
    }

    private CancellationTokenSource Linked(CancellationToken ct) =>
        CancellationTokenSource.CreateLinkedTokenSource(ct, Agent?.TurnStop ?? CancellationToken.None);

    /// <summary>The open job as the server composes it from the page's cards: the page writes no YAML, so the text is never stale.</summary>
    private OpenJob? OpenJobOf(AgentJob? job)
    {
        if (job?.Model is null) return null;
        var path = design.JobPath(job.Path);
        return new OpenJob(path, job.Unsaved || path is null, design.Design().Compose(job.Model).Yaml);
    }

    public string AgentJobs() => JsonSerializer.Serialize(new { jobs = CurrentWorkspace.Folder.Jobs() }, StudioHttp.Json);

    /// <summary>A job of the folder for the model, unless it holds a credential as a value and the person keeps it here.</summary>
    public async Task<string> AgentReadJobAsync(string? path, CancellationToken ct)
    {
        var relative = design.JobPath(path) ?? throw new StudioRefusal(400, "read-job needs the job's 'path' in the folder.");
        var job = CurrentWorkspace.Folder.ReadJob(relative);
        using var linked = Linked(ct);
        if (await Agent!.MaySendAsync(relative, job.Text, ["send", "withhold"], linked.Token) != "send")
            return Answer(new { withheld = $"The person kept the text of '{relative}' from you: it holds a credential as a value. Ask them what you need to know of it." });
        return Answer(new { path = relative, yaml = job.Text });
    }

    /// <summary>
    /// Saves a job the model wrote, as the page saves one: the text taken apart into cards and composed back, written whole
    /// against the hash the file had when the card was shown (none: the file is created, never replaced). Nothing is written
    /// without the person's "save".
    /// </summary>
    public async Task<string> AgentSaveJobAsync(string? path, string? yaml, CancellationToken ct)
    {
        var relative = design.JobPath(path) ?? throw new StudioRefusal(400, "save-job needs the job's 'path' in the folder.");
        if (!relative.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) && !relative.EndsWith(".yml", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(400, "A job is saved as a .yaml or .yml file.");
        if (string.IsNullOrWhiteSpace(yaml)) throw new StudioRefusal(400, "save-job needs the job's text as 'yamlContent'.");
        var designModel = await design.DesignForAsync(yaml, ct);
        var cards = designModel.Decompose(yaml);
        var composed = cards.Valid ? designModel.Compose(cards.Model) : cards;
        if (!composed.Valid) return Answer(new { error = "The job does not compose: " + string.Join(" ", composed.Errors) });

        var folder = CurrentWorkspace.Folder;
        string? hash = File.Exists(folder.ResolveForWrite(relative)) ? folder.ReadJob(relative).Hash : null;
        var body = new { path = relative, replaces = hash is not null, yaml = composed.Yaml, rewrites = composed.Yaml != yaml.Replace("\r\n", "\n") };
        using var linked = Linked(ct);
        var choice = await Agent!.AskAsync("save", body, null, ["save", "refuse"],
            (chosen, _) => chosen == "save" && !Unchanged(relative, hash) ? $"'{relative}' changed on disk since the card was shown: nothing was written." : null,
            linked.Token);
        return choice switch
        {
            "save" => Answer(new { saved = relative, hash = folder.WriteJob(relative, composed.Yaml, hash), yaml = composed.Yaml }),
            "refuse" => Answer(new { refused = "The person declined to save this job. Nothing was written." }),
            _ => Answer(new { error = $"'{relative}' changed on disk since the card was shown: nothing was written." }),
        };
    }

    private bool Unchanged(string relative, string? hash)
    {
        var folder = CurrentWorkspace.Folder;
        var exists = File.Exists(folder.ResolveForWrite(relative));
        return hash is null ? !exists : exists && folder.ReadJob(relative).Hash == hash;
    }

    private static string Answer(object value) => JsonSerializer.Serialize(value, StudioHttp.Json);
}
