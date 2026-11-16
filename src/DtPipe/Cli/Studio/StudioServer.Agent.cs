using System.Text.Json;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The studio's agent: its routes, and the gestures its tools make through the studio's own paths. A save is the page's
/// save (<c>Decompose</c>, <c>Compose</c>, <c>WriteJob</c> against the hash read when the card was made), which waits for the
/// person's answer to a card; no tool writes a target. Without <c>--llm-provider</c>,
/// <c>GET /api/agent</c> answers <c>enabled: false</c> and every other route 404.
/// </summary>
public sealed partial class StudioServer
{
    private StudioAgent? _agent;
    private string _slotHolder = "An action";

    /// <summary>Takes the engine slot now, or refuses naming who holds it.</summary>
    private void TakeSlot(string who)
    {
        if (!_sampling.Wait(0)) throw new StudioRefusal(409, $"{_slotHolder} is already running: wait for it to finish.");
        _slotHolder = who;
    }

    /// <summary>Waits for the engine slot, as long as the turn is not stopped; the lease releases it.</summary>
    internal async Task<IDisposable> TakeSlotAsync(string who, CancellationToken ct)
    {
        using var linked = Linked(ct);
        await _sampling.WaitAsync(linked.Token);
        _slotHolder = who;
        return new SlotLease(_sampling);
    }

    private sealed class SlotLease(SemaphoreSlim slot) : IDisposable
    {
        private int _released;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) slot.Release();
        }
    }

    private CancellationTokenSource Linked(CancellationToken ct) =>
        CancellationTokenSource.CreateLinkedTokenSource(ct, _agent?.TurnStop ?? CancellationToken.None);

    private async Task AgentRouteAsync(HttpContext context, string method, string path)
    {
        if (_agent is not { } agent)
        {
            if ((method, path) == ("GET", "/api/agent")) { await JsonAsync(context, new { enabled = false }); return; }
            throw new StudioRefusal(404, "This studio was opened without --llm-provider.");
        }
        switch (method, path)
        {
            case ("GET", "/api/agent"):
                await JsonAsync(context, agent.Status(int.TryParse(context.Request.Query["after"], out var after) ? after : 0, int.TryParse(context.Request.Query["rev"], out var rev) ? rev : 0));
                break;
            case ("GET", "/api/agent/models"):
                await JsonAsync(context, await agent.ModelsAsync(context.RequestAborted));
                break;
            case ("POST", "/api/agent/turn"):
                var turn = await BodyAsync<AgentTurnRequest>(context);
                if (string.IsNullOrWhiteSpace(turn.Prompt)) throw new StudioRefusal(400, "A turn has a prompt.");
                if (RunInProgress) throw new StudioRefusal(409, "A run is in progress: wait for it, or stop it, before asking the agent.");
                agent.StartTurn(turn.Prompt, turn.Model, OpenJobOf(turn.Job));
                context.Response.StatusCode = StatusCodes.Status202Accepted;
                await JsonAsync(context, new { started = true });
                break;
            case ("POST", "/api/agent/stop"):
                agent.Stop();
                context.Response.StatusCode = StatusCodes.Status202Accepted;
                break;
            case ("POST", "/api/agent/answer"):
                var answer = await BodyAsync<AgentAnswerRequest>(context);
                agent.Answer(answer.Id, answer.Choice, answer.Digest);
                context.Response.StatusCode = StatusCodes.Status204NoContent;
                break;
            case ("POST", "/api/agent/feedback"):
                var feedback = await BodyAsync<AgentFeedbackRequest>(context);
                agent.Feedback(feedback.Turn, feedback.Rating, feedback.Text);
                context.Response.StatusCode = StatusCodes.Status204NoContent;
                break;
            case ("DELETE", "/api/agent"):
                agent.Reset();
                context.Response.StatusCode = StatusCodes.Status204NoContent;
                break;
            default:
                throw new StudioRefusal(404, $"No {method} {path}.");
        }
    }

    /// <summary>The open job as the server composes it from the page's cards: the page writes no YAML, so the text is never stale.</summary>
    private OpenJob? OpenJobOf(AgentJob? job)
    {
        if (job?.Model is null) return null;
        var path = JobPath(job.Path);
        return new OpenJob(path, job.Unsaved || path is null, Design().Compose(job.Model).Yaml);
    }

    internal string AgentJobs() => JsonSerializer.Serialize(new { jobs = _folder.Jobs() }, Json);

    /// <summary>A job of the folder for the model, unless it holds a credential as a value and the person keeps it here.</summary>
    internal async Task<string> AgentReadJobAsync(string? path, CancellationToken ct)
    {
        var relative = JobPath(path) ?? throw new StudioRefusal(400, "read-job needs the job's 'path' in the folder.");
        var job = _folder.ReadJob(relative);
        using var linked = Linked(ct);
        if (await _agent!.MaySendAsync(relative, job.Text, ["send", "withhold"], linked.Token) != "send")
            return Answer(new { withheld = $"The person kept the text of '{relative}' from you: it holds a credential as a value. Ask them what you need to know of it." });
        return Answer(new { path = relative, yaml = job.Text });
    }

    /// <summary>
    /// Saves a job the model wrote, as the page saves one: the text taken apart into cards and composed back, written whole
    /// against the hash the file had when the card was shown (none: the file is created, never replaced). Nothing is written
    /// without the person's "save".
    /// </summary>
    internal async Task<string> AgentSaveJobAsync(string? path, string? yaml, CancellationToken ct)
    {
        var relative = JobPath(path) ?? throw new StudioRefusal(400, "save-job needs the job's 'path' in the folder.");
        if (!relative.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) && !relative.EndsWith(".yml", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(400, "A job is saved as a .yaml or .yml file.");
        if (string.IsNullOrWhiteSpace(yaml)) throw new StudioRefusal(400, "save-job needs the job's text as 'yamlContent'.");
        var design = await DesignForAsync(yaml, ct);
        var cards = design.Decompose(yaml);
        var composed = cards.Valid ? design.Compose(cards.Model) : cards;
        if (!composed.Valid) return Answer(new { error = "The job does not compose: " + string.Join(" ", composed.Errors) });

        string? hash = File.Exists(_folder.ResolveForWrite(relative)) ? _folder.ReadJob(relative).Hash : null;
        var body = new { path = relative, replaces = hash is not null, yaml = composed.Yaml, rewrites = composed.Yaml != yaml.Replace("\r\n", "\n") };
        using var linked = Linked(ct);
        var choice = await _agent!.AskAsync("save", body, null, ["save", "refuse"],
            (chosen, _) => chosen == "save" && !Unchanged(relative, hash) ? $"'{relative}' changed on disk since the card was shown: nothing was written." : null,
            linked.Token);
        return choice switch
        {
            "save" => Answer(new { saved = relative, hash = _folder.WriteJob(relative, composed.Yaml, hash), yaml = composed.Yaml }),
            "refuse" => Answer(new { refused = "The person declined to save this job. Nothing was written." }),
            _ => Answer(new { error = $"'{relative}' changed on disk since the card was shown: nothing was written." }),
        };
    }

    private bool Unchanged(string relative, string? hash)
    {
        var exists = File.Exists(_folder.ResolveForWrite(relative));
        return hash is null ? !exists : exists && _folder.ReadJob(relative).Hash == hash;
    }

    private static string Answer(object value) => JsonSerializer.Serialize(value, Json);

    private sealed record AgentJob(string? Path, bool Unsaved, DesignModel? Model);

    private sealed record AgentTurnRequest(string? Prompt, string? Model, AgentJob? Job);

    private sealed record AgentAnswerRequest(string? Id, string? Choice, string? Digest);

    private sealed record AgentFeedbackRequest(int Turn, string? Rating, string? Text);
}
