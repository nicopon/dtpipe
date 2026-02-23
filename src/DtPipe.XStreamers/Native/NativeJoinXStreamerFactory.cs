
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;
using DtPipe.Core.Attributes;

namespace DtPipe.XStreamers.Native;

public class NativeJoinXStreamerFactory : IProviderDescriptor<IStreamReader>
{
    public NativeJoinXStreamerFactory()
    {
    }

    public string ComponentName => "native-join";
    public string Category => "XStreamers";
    public Type OptionsType => typeof(NativeJoinOptions);
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

public class NativeJoinOptions : IOptionSet
{
    public static string Prefix => "native-join";
    public static string DisplayName => "Native Join";

    [ComponentOption("--main")]
    public string[] MainAlias { get; set; } = Array.Empty<string>();

    [ComponentOption("--ref")]
    public string[] RefAlias { get; set; } = Array.Empty<string>();

    [ComponentOption("--on")]
    public string[] On { get; set; } = Array.Empty<string>();

    [ComponentOption("--join-type")]
    public string[] JoinType { get; set; } = Array.Empty<string>();

    [ComponentOption("--select")]
    public string[] Select { get; set; } = Array.Empty<string>();
}
