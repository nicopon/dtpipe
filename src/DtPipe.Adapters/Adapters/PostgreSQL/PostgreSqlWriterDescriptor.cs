using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "pg";
    public string Category => "Writer Options";
    public Type OptionsType => typeof(PostgreSqlWriterOptions);
    public bool CanHandle(string connectionString) => connectionString.StartsWith("postgres:", StringComparison.OrdinalIgnoreCase) || connectionString.StartsWith("postgresql:", StringComparison.OrdinalIgnoreCase) || connectionString.Contains("Host=", StringComparison.OrdinalIgnoreCase);
    public bool RequiresQuery => true;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (PostgreSqlWriterOptions)options;
        return new PostgreSqlDataWriter(connectionString, opt, serviceProvider.GetRequiredService<ILogger<PostgreSqlDataWriter>>(), new PostgreSqlTypeConverter());
    }
}
