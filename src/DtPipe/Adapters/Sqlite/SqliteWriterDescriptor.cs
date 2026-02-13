using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Sqlite;

public class SqliteWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => "sqlite";

	public Type OptionsType => typeof(SqliteWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return SqliteConnectionHelper.CanHandle(connectionString);
	}

	public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var sqliteOptions = (SqliteWriterOptions)options;
		var logger = serviceProvider.GetRequiredService<ILogger<SqliteDataWriter>>();

		var dsConnectionString = SqliteConnectionHelper.ToDataSourceConnectionString(connectionString);
		return new SqliteDataWriter(dsConnectionString, sqliteOptions, logger, SqliteTypeConverter.Instance);
	}
}
