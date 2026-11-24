using Jint;

namespace DtPipe.Transformers.Services;

public class JsEngineProvider : IJsEngineProvider
{
    private readonly ThreadLocal<Engine> _threadLocalEngine;

    public JsEngineProvider()
    {
        _threadLocalEngine = new ThreadLocal<Engine>(CreateEngine, trackAllValues: true);
    }

    public bool EnginePerThread => true;

    public Engine GetEngine()
    {
        return _threadLocalEngine.Value!;
    }

    private Engine CreateEngine()
    {
        // Shared configuration for all JS execution in DtPipe
        var engine = new Engine(cfg => cfg
            .Strict(true)
            .LimitMemory(256_000_000) // 256MB memory limit for streaming JS pipelines
            .TimeoutInterval(TimeSpan.FromSeconds(5))
        );

        // An engine serves every script of its thread for as long as the provider lives, so its realm is
        // frozen before the first script: none can leave state there for the next call.
        JsRealmFreeze.Apply(engine);
        return engine;
    }

    public void Dispose()
    {
        // Dispose all created engines
        if (_threadLocalEngine.IsValueCreated)
        {
            foreach (var engine in _threadLocalEngine.Values)
            {
                engine.Dispose();
            }
        }
        _threadLocalEngine.Dispose();
    }
}
