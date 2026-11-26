using Jint;
using Jint.Native;
using Jint.Runtime;

namespace DtPipe.Transformers.Services;

/// <summary>
/// Compiles a transformer's JavaScript snippet via <c>Function</c> so the text is parsed strictly as a
/// function body and can never close a wrapper to declare bindings in the global lexical environment.
/// </summary>
internal static class JsScriptCompiler
{
    /// <summary>
    /// Compiles <paramref name="script"/> as a function taking <paramref name="parameterName"/>.
    /// An input that parses as a single expression is returned directly; anything else is compiled as statements.
    /// </summary>
    public static (JsValue Function, string CompiledBody) Compile(Engine engine, string parameterName, string script)
    {
        var body = script.Trim();
        var expression = body.TrimEnd(';').Trim();

        if (expression.Length > 0)
        {
            var exprBody = $"\"use strict\";\nreturn ({expression}\n);";
            try
            {
                return (CompilePrepared(engine, parameterName, exprBody), exprBody);
            }
            catch (JavaScriptException)
            {
                // Parsing as a single expression throws SyntaxError when the script is a statement block.
            }
        }

        var stmtBody = $"\"use strict\";\n{body}";
        return (CompilePrepared(engine, parameterName, stmtBody), stmtBody);
    }

    public static JsValue CompilePrepared(Engine engine, string parameterName, string compiledBody) =>
        engine.Intrinsics.Function.Construct([JsString.Create(parameterName), JsString.Create(compiledBody)], engine.Intrinsics.Function);

    public static string AsFunctionExpression(string parameterName, string compiledBody) =>
        $"function({parameterName}) {{ {compiledBody} }}";
}
