using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Services;

internal static class JsEngineExtensions
{
    /// <summary>
    /// Calls <paramref name="function"/> and runs the promise reactions the call queued before it returns.
    /// The engine would keep them until its next evaluation, which would run them inside the next script's
    /// time limit: a script's own reaction is run, and fails, in its own call.
    /// </summary>
    public static JsValue CallAndSettle(this Engine engine, JsValue function, JsValue thisObject, JsValue[] arguments)
    {
        var result = engine.Call(function, thisObject, arguments);
        engine.Advanced.ProcessTasks();
        return result;
    }
}
