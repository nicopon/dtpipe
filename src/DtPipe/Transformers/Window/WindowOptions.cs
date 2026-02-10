using DtPipe.Cli.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Window;

public class WindowOptions : ITransformerOptions
{
	public static string Prefix => "window";
	public static string DisplayName => "Window Transformer";

	[CliOption("--window-count", Description = "Number of rows to accumulate before processing window script")]
	public int? Count { get; set; }

	[CliOption("--window-key", Description = "Column name to use for key-based windowing (flush when key changes)")]
	public string? Key { get; set; }

	[CliOption("--window-script", Description = "Javascript script to execute on the accumulated window (variable 'rows'). Must return array of rows.")]
	public string? Script { get; set; }
}
