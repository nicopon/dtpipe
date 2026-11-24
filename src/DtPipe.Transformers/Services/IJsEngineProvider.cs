using Jint;

namespace DtPipe.Transformers.Services;

/// <summary>
/// Provides a shared Jint Engine instance for the current scope/thread.
/// </summary>
public interface IJsEngineProvider : IDisposable
{
    /// <summary>
    /// Gets the Jint Engine instance.
    /// The engine is initialized on first access and reused within the scope/thread.
    /// </summary>
    Engine GetEngine();

    /// <summary>
    /// Whether <see cref="GetEngine"/> gives each thread an engine of its own, so scripts may run on several
    /// threads at once. A provider that hands one engine to every caller keeps the default and is used from
    /// one thread at a time.
    /// </summary>
    bool EnginePerThread => false;
}
