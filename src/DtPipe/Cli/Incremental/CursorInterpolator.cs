using DtPipe.Cli.Expressions;
using DtPipe.Adapters.Shared.Cursor;
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
        // 1. A value given for the run
        if (CursorOverride.Value is { } given)
            return given;

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
        if (EdgesOnlyScope.IsArmed)
            throw new InvalidOperationException(EdgesOnlyScope.FileRefusal);

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
