namespace DtPipe.Coordinator.Server;

/// <summary>
/// A scenario of the Lab view, with the layout it proposes: <see cref="Cuts"/> and
/// <see cref="Placement"/> preload the page, the user changes them freely.
/// </summary>
public sealed record CatalogEntry(
    string Id, string Title, string Description, string Yaml,
    IReadOnlyList<CutRequest> Cuts, IReadOnlyDictionary<string, string> Placement);

/// <summary>
/// The Lab view's scenarios: the monolithic jobs in <see cref="CoordinatorServerOptions.Scenarios"/>,
/// read in lab mode only. A file's leading <c>#</c> comment lines give its title (the first) and
/// description (the rest); dtpipe reads past them. Directives among them preload the page
/// (<c># lab-cut: branch@stage, ...</c>, <c># lab-place: unit=node, ...</c>), and
/// <c># lab-check: &lt;SQL&gt;</c> names the query a test compares between a run and its witness.
/// </summary>
public sealed class PipelineCatalog(CoordinatorServerOptions options)
{
    private const string CutDirective = "lab-cut:";
    private const string PlaceDirective = "lab-place:";
    private const string CheckDirective = "lab-check:";

    public IReadOnlyList<CatalogEntry> List()
    {
        var dir = options.Scenarios;
        if (!options.LabMode || dir is null || !Directory.Exists(dir)) return [];
        return Directory.GetFiles(dir, "*.yaml").Order(StringComparer.Ordinal).Select(Load).ToList();
    }

    private static CatalogEntry Load(string path)
    {
        var yaml = File.ReadAllText(path);
        var header = yaml.Split('\n').TakeWhile(l => l.StartsWith('#')).Select(l => l.TrimStart('#').Trim()).ToList();
        var prose = header.Where(l => !l.StartsWith(CutDirective) && !l.StartsWith(PlaceDirective) && !l.StartsWith(CheckDirective)).ToList();

        var cuts = Directive(header, CutDirective)
            .Select(item => item.Split('@'))
            .Where(parts => parts.Length == 2 && int.TryParse(parts[1], out _))
            .Select(parts => new CutRequest(parts[0].Trim(), int.Parse(parts[1])))
            .ToList();
        var placement = Directive(header, PlaceDirective)
            .Select(item => item.Split('='))
            .Where(parts => parts.Length == 2)
            .ToDictionary(parts => parts[0].Trim(), parts => parts[1].Trim(), StringComparer.Ordinal);

        var id = Path.GetFileNameWithoutExtension(path);
        return new CatalogEntry(id, prose.FirstOrDefault() ?? id, string.Join(' ', prose.Skip(1)).Trim(), yaml, cuts, placement);
    }

    private static IEnumerable<string> Directive(IEnumerable<string> header, string directive) =>
        header.Where(l => l.StartsWith(directive))
            .SelectMany(l => l[directive.Length..].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
}
