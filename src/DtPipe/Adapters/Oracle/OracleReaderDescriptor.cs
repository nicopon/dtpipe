using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Oracle;

public class OracleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
	public string ProviderName => OracleConstants.ProviderName;

	public Type OptionsType => typeof(OracleReaderOptions);

	public bool RequiresQuery => true;

	public bool CanHandle(string connectionString)
	{
		return OracleConnectionHelper.CanHandle(connectionString);
	}

	public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		return new OracleStreamReader(
			OracleConnectionHelper.GetConnectionString(connectionString),
			context.Query!,
			(OracleReaderOptions)options,
			context.QueryTimeout);
	}
}
