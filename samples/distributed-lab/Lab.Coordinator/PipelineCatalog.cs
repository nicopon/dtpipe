namespace DtPipe.Lab.Coordinator;

/// <summary>
/// A catalog job, with the layout the lab proposes for it: <see cref="Cuts"/> and
/// <see cref="Placement"/> preload the page, the user changes them freely.
/// </summary>
public sealed record CatalogEntry(
    string Id, string Title, string Description, string Yaml,
    IReadOnlyList<CutRequest> Cuts, IReadOnlyDictionary<string, string> Placement);

/// <summary>
/// The monolithic jobs under <c>pipelines/</c>. A file's leading <c>#</c> comment lines give its
/// title (the first) and description (the rest); dtpipe reads past them. Directives among them are
/// the lab's: <c># lab-cut: branch@stage, ...</c> and <c># lab-place: unit=node, ...</c> preload the
/// page; <c># lab-check: &lt;SQL&gt;</c> is the query <c>smoke.py</c> compares against the witness.
/// </summary>
public sealed class PipelineCatalog(LabOptions options)
{
    private const string CutDirective = "lab-cut:";
    private const string PlaceDirective = "lab-place:";
    private const string CheckDirective = "lab-check:";

    public IReadOnlyList<CatalogEntry> List()
    {
        var dir = Path.Combine(options.LabRoot, "pipelines");
        if (!Directory.Exists(dir)) return [];
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
