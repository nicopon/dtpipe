using DtPipe.Transformers.Services;
using Jint;

namespace DtPipe.Tests.Helpers;

/// <summary>The engine's limits, scaled down so that a test reaches them in well under a second.</summary>
internal sealed class LimitedJsEngineProvider : IJsEngineProvider
{
    private readonly ThreadLocal<Jint.Engine> _engine = new(
        () => new Jint.Engine(cfg => cfg.Strict(true).LimitMemory(8_000_000).TimeoutInterval(TimeSpan.FromMilliseconds(300))),
        trackAllValues: true);

    public Jint.Engine GetEngine() => _engine.Value!;

    public void Dispose()
    {
        foreach (var engine in _engine.Values)
            engine.Dispose();
        _engine.Dispose();
    }
}
