using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Project;

public class ProjectOptions : ITransformerOptions
{
	public static string Prefix => "project";
	public static string DisplayName => "Projection Transformer";

	[CliOption("--project", Description = "Keep only specified columns (comma-separated). Whitelist strategy.")]
	public string? Project { get; set; }

	[CliOption("--drop", Description = "Remove specified columns (comma-separated). Blacklist strategy.")]
	public string? Drop { get; set; }
}
