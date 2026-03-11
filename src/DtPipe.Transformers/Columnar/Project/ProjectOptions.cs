using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Columnar.Project;

public class ProjectOptions : ITransformerOptions
{
	public static string Prefix => "project";
	public static string DisplayName => "Projection Transformer";

	[ComponentOption("--project", Description = "Keep only specified columns. repeatable.")]
	public IEnumerable<string> Project { get; set; } = Array.Empty<string>();

	[ComponentOption("--drop", Description = "Remove specified columns. repeatable.")]
	public IEnumerable<string> Drop { get; set; } = Array.Empty<string>();

    [ComponentOption("--rename", Description = "Rename columns (Old:New). repeatable.")]
    public IEnumerable<string> Rename { get; set; } = Array.Empty<string>();
}
