using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Row.Window;

public class WindowOptions : ITransformerOptions
{
	public static string Prefix => "window";
	public static string DisplayName => "Window Transformer";

	[ComponentOption("--window-count", Description = "Number of rows to accumulate before processing window script")]
	public int? Count { get; set; }

	[ComponentOption("--window-key", Description = "Column name to use for key-based windowing (flush when key changes)")]
	public string? Key { get; set; }

	[ComponentOption("--window-script", Description = "Javascript script to execute on the accumulated window (variable 'rows'). Must return array of rows.")]
	public string? Script { get; set; }
}
