using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.Oracle;

public class OracleWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => OracleConstants.ProviderName;

	public Type OptionsType => typeof(OracleWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return OracleConnectionHelper.CanHandle(connectionString);
	}

	public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
	{
		var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
		return new OracleDataWriter(connectionString, (OracleWriterOptions)options, loggerFactory.CreateLogger<OracleDataWriter>(), OracleTypeConverter.Instance);
	}
}
