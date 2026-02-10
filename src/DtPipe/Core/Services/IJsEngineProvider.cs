using Jint;

namespace DtPipe.Core.Services;

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
}
