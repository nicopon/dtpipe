using DtPipe.Core.Abstractions;
using DtPipe.Adapters.Shared.Dialects;

namespace DtPipe.Adapters.MySql;

public class MySqlReaderDescriptor : IProviderDescriptor<IStreamReader>, IHasSqlDialect
{
    public string ComponentName => MySqlMetadata.ComponentName;
    public string Category => "Reader Options";
    public Type OptionsType => typeof(MySqlReaderOptions);
    public bool CanHandle(string connectionString) => MySqlMetadata.CanHandle(connectionString);
    public bool SupportsStdio => MySqlMetadata.SupportsStdio;
    public bool RequiresQuery => true;
    public bool YieldsColumnarOutput => true;

    public ISqlDialect Dialect => new MySqlDialect();

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        var opt = (MySqlReaderOptions)options;
        return new MySqlReader(connectionString, opt.Query!, opt.QueryTimeout);
    }
}
