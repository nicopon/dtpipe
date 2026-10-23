using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using DtPipe.Core.Dialects;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>, IHasSqlDialect
{
    public string ComponentName => SqlServerMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(SqlServerReaderOptions);
    public bool CanHandle(string connectionString) => SqlServerMetadata.CanHandle(connectionString);
    public bool SupportsStdio => SqlServerMetadata.SupportsStdio;
    public bool RequiresQuery => true;
    public bool YieldsColumnarOutput => true;

    public ISqlDialect Dialect => new SqlServerDialect();

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (SqlServerReaderOptions)options;
        return new SqlServerReader(connectionString, opt.Query!, opt, 0);
    }
}
