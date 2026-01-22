using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlDataWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public PostgreSqlDataWriterFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "postgresql-writer";
    public override string Category => "Writer Options";
    public string SupportedExtension => ".dump";

    public bool CanHandle(string connectionString)
    {
        return PostgreSqlConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var pgOptions = Registry.Get<PostgreSqlWriterOptions>();
        return new PostgreSqlDataWriter(
            PostgreSqlConnectionHelper.GetConnectionString(options.OutputPath),
            pgOptions
        );
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(PostgreSqlWriterOptions);
    }
}
