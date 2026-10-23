using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Row.Window;

[Description("Buffers rows into fixed-size or key-based windows and runs a JavaScript script over each accumulated batch.")]
[ComponentHelp(
    usageNotes: "In YAML, use the 'options' block for 'count' (rows per window), 'key' (flush when this column's value changes), and 'script' (JS executed on the buffered 'rows' array). The script must return an array of row objects, one per output row.",
    examples: new[] {
        "transformers:\n  - type: window\n    options:\n      count: 5\n      script: \"rows.map(r => ({ ...r, rolling_avg: rows.reduce((s, x) => s + x.val, 0) / rows.length }))\""
    })]
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
