using System.Text.Json.Nodes;
using DtPipe.Jobs;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The cards as the coordinator's designer page sends them. The page names the lab's check query
/// <see cref="Check"/>; the job keeps it as its <c>lab-check:</c> header directive, which dtpipe's
/// model carries without reading it.
/// </summary>
internal sealed record PageDesignModel(
    string Title, string? Description, string? Check, IReadOnlyList<DesignStep> Steps, JsonObject? Layout = null)
{
    private const string CheckDirective = "lab-check:";

    public DesignModel ToModel() =>
        new(Title, Description, string.IsNullOrWhiteSpace(Check) ? [] : [$"{CheckDirective} {Check}"], Steps, Layout);

    public static PageDesignModel From(DesignModel model) => new(
        model.Title, model.Description,
        model.Directives?.FirstOrDefault(d => d.StartsWith(CheckDirective, StringComparison.Ordinal))?[CheckDirective.Length..].Trim(),
        model.Steps, model.Layout);
}

internal sealed record PageDesignResult(
    PageDesignModel Model, string Yaml, IReadOnlyList<StepInfo> Steps, IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings, bool Valid)
{
    public static PageDesignResult From(DesignResult result) =>
        new(PageDesignModel.From(result.Model), result.Yaml, result.Steps, result.Errors, result.Warnings, result.Valid);
}
