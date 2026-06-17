using DtPipe.Core.Expressions;

namespace DtPipe.Cli.Expressions;

public sealed class EnvVarInterpolator : IStringInterpolator
{
    public Task<string?> TryResolveAsync(string expression, CancellationToken ct)
    {
        // Try to resolve environment variable
        var envValue = Environment.GetEnvironmentVariable(expression);
        if (envValue != null)
        {
            return Task.FromResult<string?>(envValue);
        }
        
        return Task.FromResult<string?>(null);
    }
}
