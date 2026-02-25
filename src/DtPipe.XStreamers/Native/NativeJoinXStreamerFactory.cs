using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;

namespace DtPipe.XStreamers.Native;

public class NativeJoinXStreamerFactory : IXStreamerFactory
{
    public NativeJoinXStreamerFactory()
    {
    }

    public string ComponentName => "native-join";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(NativeJoinOptions);
    public XStreamerChannelMode ChannelMode => XStreamerChannelMode.Native;
    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString) => false;

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opts = (NativeJoinOptions)options;
        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<NativeJoinXStreamer>();
        return new NativeJoinXStreamer(
            registry,
            opts.MainAlias?.LastOrDefault() ?? "",
            opts.RefAlias?.LastOrDefault() ?? "",
            opts.On?.LastOrDefault() ?? "",
            opts.JoinType?.LastOrDefault() ?? "Inner",
            opts.Select?.LastOrDefault() ?? "",
            logger);
    }
}

public class NativeJoinOptions : IOptionSet, ICliOptionMetadata
{
    public static string Prefix => "native-join";
    public static string DisplayName => "Native Join";

    public string[] MainAlias { get; set; } = Array.Empty<string>();

    public string[] RefAlias { get; set; } = Array.Empty<string>();

    public string[] On { get; set; } = Array.Empty<string>();

    public string[] JoinType { get; set; } = Array.Empty<string>();

    public string[] Select { get; set; } = Array.Empty<string>();

    public IReadOnlyDictionary<string, string> PropertyToFlag => new Dictionary<string, string>
    {
        { nameof(MainAlias), "--main" },
        { nameof(RefAlias), "--ref" },
        { nameof(On), "--on" },
        { nameof(JoinType), "--join-type" },
        { nameof(Select), "--select" }
    };
}
