using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => SqlServerConstants.ProviderName;
	public Type OptionsType => typeof(SqlServerWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.StartsWith("mssql:", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var sqlOptions = (SqlServerWriterOptions)options;
		var logger = serviceProvider.GetRequiredService<ILogger<SqlServerDataWriter>>();
		return new SqlServerDataWriter(connectionString, sqlOptions, logger, SqlServerTypeConverter.Instance);
	}
}
