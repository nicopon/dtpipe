using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Sqlite;

public class SqliteDataWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public SqliteDataWriterFactory(OptionsRegistry registry) : base(registry)
    {
        // Register default options
        if (!Registry.Has<SqliteWriterOptions>())
        {
            Registry.Register(new SqliteWriterOptions());
        }
    }

    public override string ProviderName => "sqlite-writer";
    public override string Category => "Writer Options";
    public string SupportedExtension => ".sqlite";

    public bool CanHandle(string outputPath)
    {
        return SqliteConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = SqliteConnectionHelper.ToDataSourceConnectionString(options.OutputPath);
        return new SqliteDataWriter(connectionString, Registry);
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(SqliteWriterOptions);
    }
}
