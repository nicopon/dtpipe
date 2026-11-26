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
        Exception? callException = null;
        try
        {
            return engine.Call(function, thisObject, arguments);
        }
        catch (Exception ex)
        {
            callException = ex;
            throw;
        }
        finally
        {
            try
            {
                engine.Advanced.ProcessTasks();
            }
            catch (Exception) when (callException is not null and not Jint.Runtime.JavaScriptException)
            {
                // Preserve a fatal engine constraint fault from Call rather than masking it.
            }
        }
    }
}
