using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.MemoryChannel;

[Description("Internal in-process Arrow channel used to pipe RecordBatches between DAG branches with zero-copy columnar passthrough.")]
[ComponentHelp(
    usageNotes: "This is internal plumbing wired by the DAG engine to connect branches declared via '--alias' / '--from' (or 'from:' in YAML) — the connection string is an internal channel alias, not a value end users write themselves. Used for the columnar path, passing Arrow RecordBatches between branches without converting to rows.",
    examples: new[] {
        "main:\n  input: \"events.parquet\"\narchive:\n  from: \"main\"\n  output: \"archive.parquet\"\nlive:\n  from: \"main\"\n  output: \"live.parquet\""
    })]
public class ArrowMemoryChannelOptions : IOptionSet
{
    public static string Prefix => "arrow-memory";
    public static string DisplayName => "Arrow Memory Channel";
}
