using DtPipe.Jobs;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Arms a library pipeline from what the library holds. A reference whose version no check has pinned is
/// <see cref="BindingCauses.Unchecked"/> and blocks the arming, so the arming runs the designer's check for it and writes the pin it
/// finds, in a commit of its own: a pin moves only by a person's act, and arming is one. A pin that exists is never moved here,
/// so a brick that changed since its check (<see cref="BindingCauses.BrickChanged"/>) still waits for a save. The armed commit and
/// bindings are those of the library after the pins are written.
/// </summary>
internal sealed class LibraryArming(PipelineLibrary library, DesignValidator validator, DeploymentManager deployments, EventBus bus)
{
    public async Task<ArmedDeployment> ArmAsync(LibraryDocument doc, string armedBy, CancellationToken ct)
    {
        var id = doc.Id;
        var unpinned = doc.BindingProblems.Where(p => p.Cause == BindingCauses.Unchecked).Select(p => p.Alias).ToHashSet(StringComparer.Ordinal);
        if (unpinned.Count > 0)
        {
            Refuse(id, doc.BindingProblems.Where(p => p.Blocking && !unpinned.Contains(p.Alias)));
            doc = await PinAsync(doc, unpinned, ct);
        }
        Refuse(id, doc.BindingProblems.Where(p => p.Blocking));
        var armed = new ArmedDeployment(
            id, doc.Commit!, PipelineLibrary.Sha(doc.Yaml), Bindings.Sha(doc.Bindings), doc.Bindings, DateTimeOffset.UtcNow, armedBy);
        await deployments.ArmAsync(armed, doc.Yaml, ct);
        return armed;
    }

    /// <summary>The document after the check pinned <paramref name="aliases"/>: only their entries change.</summary>
    private async Task<LibraryDocument> PinAsync(LibraryDocument doc, IReadOnlySet<string> aliases, CancellationToken ct)
    {
        DesignCheck check;
        try { check = await validator.ValidateAsync(doc.Yaml, doc.Bindings, ct); }
        catch (Exception ex) when (ex is not OperationCanceledException) { check = new DesignCheck(false, $"The job is not readable: {ex.Message}"); }
        if (!check.Ok) throw new ConflictException($"'{doc.Id}' could not be armed: the designer's check refused its job.\n{check.Report}");

        var referenced = BrickReferences.In(doc.Yaml).Keys.ToHashSet(StringComparer.Ordinal);
        var pinned = Bindings.Pinned(doc.Bindings, check.Pins, referenced);
        var bindings = doc.Bindings.ToDictionary(b => b.Key, b => aliases.Contains(b.Key) ? pinned[b.Key] : b.Value, StringComparer.Ordinal);
        var saved = await library.SaveBindingsAsync(doc.Id, bindings, $"Pin the bricks of {doc.Id} to arm it", ct);
        bus.Publish("library", new { id = doc.Id, commit = saved.Commit });
        return await library.GetAsync(doc.Id, ct) ?? throw new ConflictException($"'{doc.Id}' is not in the library.");
    }

    private static void Refuse(string id, IEnumerable<BindingProblem> problems)
    {
        if (problems.Select(p => p.Message).ToList() is { Count: > 0 } refused)
            throw new ConflictException($"The bindings of '{id}' do not hold: " + string.Join(" ", refused));
    }
}
