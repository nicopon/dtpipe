using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using DtPipe.Core.Dialects;

namespace DtPipe.Adapters.Sqlite;

public class SqliteReaderDescriptor : IProviderDescriptor<IStreamReader>, IHasSqlDialect
{
    public string ComponentName => SqliteMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(SqliteReaderOptions);
    public bool CanHandle(string connectionString) => SqliteMetadata.CanHandle(connectionString);
    public bool SupportsStdio => SqliteMetadata.SupportsStdio;
    public bool RequiresQuery => true;
    public bool YieldsColumnarOutput => true;

    public ISqlDialect Dialect => new SqliteDialect();

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqliteReaderOptions)options;
        return new SqliteReader(SqliteConnectionHelper.ToDataSourceConnectionString(connectionString), opt.Query!, 0);
    }
}
