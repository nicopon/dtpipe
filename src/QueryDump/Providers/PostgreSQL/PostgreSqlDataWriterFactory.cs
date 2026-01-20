using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Providers.PostgreSQL;

public class PostgreSqlDataWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public PostgreSqlDataWriterFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "postgresql-writer";
    public override string Category => "PostgreSQL Writer Options";
    public string SupportedExtension => ".dump"; // Or .sql? Maybe .dump is safer for binary/text copy.

    public bool CanHandle(string connectionString)
    {
        return PostgreSqlConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var pgOptions = Registry.Get<PostgreSqlOptions>();
        return new PostgreSqlDataWriter(
            PostgreSqlConnectionHelper.GetConnectionString(options.OutputPath),
            pgOptions
        );
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(PostgreSqlOptions);
    }
}
