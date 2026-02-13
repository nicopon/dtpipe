using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.PostgreSQL;

public class PostgreSqlWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => PostgreSqlConstants.ProviderName;

	public Type OptionsType => typeof(PostgreSqlWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return PostgreSqlConnectionHelper.CanHandle(connectionString);
	}

	public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var logger = serviceProvider.GetRequiredService<ILogger<PostgreSqlDataWriter>>();
		return new PostgreSqlDataWriter(
			PostgreSqlConnectionHelper.GetConnectionString(connectionString),
			(PostgreSqlWriterOptions)options,
			logger,
			PostgreSqlTypeConverter.Instance
		);
	}
}
