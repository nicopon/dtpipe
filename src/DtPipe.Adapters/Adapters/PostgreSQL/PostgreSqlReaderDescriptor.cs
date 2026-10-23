using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using DtPipe.Core.Dialects;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlReaderDescriptor : IProviderDescriptor<IStreamReader>, IHasSqlDialect
{
    public string ComponentName => PostgreSqlMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(PostgreSqlReaderOptions);
    public bool CanHandle(string connectionString) => PostgreSqlMetadata.CanHandle(connectionString);
    public bool SupportsStdio => PostgreSqlMetadata.SupportsStdio;
    public bool RequiresQuery => true;
    public bool YieldsColumnarOutput => true;

    public ISqlDialect Dialect => new PostgreSqlDialect();

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (PostgreSqlReaderOptions)options;
        return new PostgreSqlReader(connectionString, opt.Query!, 0);
    }
}
