using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.MemoryChannel;

public class ArrowMemoryChannelOptions : IOptionSet
{
    public static string Prefix => "arrow-memory";
    public static string DisplayName => "Arrow Memory Channel";

    [Description("Internal buffer size for column batching before emitting a RecordBatch")]
    public int BatchSize { get; set; } = 10000;
}
