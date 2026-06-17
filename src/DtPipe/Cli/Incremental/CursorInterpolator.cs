using DtPipe.Core.Cursor;
using DtPipe.Core.Expressions;

namespace DtPipe.Cli.Incremental;

public sealed class CursorInterpolator : IStringInterpolator
{
    public Task<string?> TryResolveAsync(string expression, CancellationToken ct)
    {
        if (expression.StartsWith("cursor://", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult<string?>(ResolveCursorExpression(expression["cursor://".Length..]));
        }

        return Task.FromResult<string?>(null);
    }

    private string ResolveCursorExpression(string expressionBody)
    {
        // 1. Check if an environment override is active
        var envOverride = Environment.GetEnvironmentVariable("DTPIPE_CURSOR_OVERRIDE");
        if (!string.IsNullOrWhiteSpace(envOverride))
            return envOverride;

        // 2. Parse path|default
        string path = expressionBody;
        string? defaultValue = null;

        var pipeIndex = expressionBody.IndexOf('|');
        if (pipeIndex >= 0)
        {
            path = expressionBody[..pipeIndex];
            defaultValue = expressionBody[(pipeIndex + 1)..];
        }

        if (string.IsNullOrWhiteSpace(path))
            return defaultValue ?? "";

        // 3. Read state file
        var cursorValue = CursorStateStore.Read(path);
        if (cursorValue != null && !string.IsNullOrEmpty(cursorValue.Value))
        {
            return cursorValue.Value;
        }

        // 4. Return default if file not found
        return defaultValue ?? "";
    }
}
