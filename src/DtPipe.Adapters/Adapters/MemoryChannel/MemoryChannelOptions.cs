using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.MemoryChannel;

[Description("Internal in-process channel used to pipe row batches between DAG branches.")]
[ComponentHelp(
    usageNotes: "This is internal plumbing wired by the DAG engine to connect branches declared via '--alias' / '--from' (or 'from:' in YAML) — the connection string is an internal channel alias, not a value end users write themselves. Consumers see plain row batches; data is bridged to/from Arrow RecordBatches internally.",
    examples: new[] {
        "main:\n  input: \"events.csv\"\narchive:\n  from: \"main\"\n  output: \"archive.csv\"\nlive:\n  from: \"main\"\n  output: \"live.csv\""
    })]
public class MemoryChannelOptions : IOptionSet
{
    public static string Prefix => "mem";
    public static string DisplayName => "Memory Channel";
}
