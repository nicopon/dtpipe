using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

public class SqlServerReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => SqlServerConstants.ProviderName;

	public Type OptionsType => typeof(SqlServerReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return SqlServerConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		return new SqlServerStreamReader(
			SqlServerConnectionHelper.GetConnectionString(connectionString),
			context.Query!,
			(SqlServerReaderOptions)options,
			context.QueryTimeout);
	}
}
