using System.ComponentModel;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Attributes;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Adapters.Null;

public class NullDataWriterFactory : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => NullMetadata.ComponentName;
    public string Category => "Writer Options";
    public Type OptionsType => typeof(NullDataWriterOptions);

    public bool CanHandle(string connectionString) => NullMetadata.CanHandle(connectionString);
    public bool SupportsStdio => NullMetadata.SupportsStdio;

    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        return new NullDataWriter();
    }
}

[Description("Discards all rows without writing anything, useful for benchmarking reader and transformer throughput.")]
[ComponentHelp(
    usageNotes: "Connection string is simply 'null:' (or the bare 'null' component name) — no configuration needed. Use it as the output target to measure source and transform performance without the overhead of a real destination.",
    examples: new[] {
        "main:\n  input: \"generate:5m\"\n  output: \"null:\""
    })]
public class NullDataWriterOptions : IWriterOptions
{
    public static string Prefix => "null";
    public static string DisplayName => "Null Data Writer";
}
