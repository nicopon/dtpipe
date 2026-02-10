using Jint;

namespace DtPipe.Core.Services;

public class JsEngineProvider : IJsEngineProvider
{
	private readonly ThreadLocal<Engine> _threadLocalEngine;

	public JsEngineProvider()
	{
		_threadLocalEngine = new ThreadLocal<Engine>(CreateEngine, trackAllValues: true);
	}

	public Engine GetEngine()
	{
		return _threadLocalEngine.Value!;
	}

	private Engine CreateEngine()
	{
		// Shared configuration for all JS execution in DtPipe
		return new Engine(cfg => cfg
			.Strict(true)
			.LimitMemory(50_000_000) // 50MB limit (increased from 20MB for safety with windows/arrays)
			.TimeoutInterval(TimeSpan.FromSeconds(5)) // Increased timeout for heavier operations
		);
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
