using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Adapters.Checksum;

public class ChecksumWriterDescriptor : IProviderDescriptor<IDataWriter>
{
	public string ProviderName => ChecksumConstants.ProviderName;
	public Type OptionsType => typeof(ChecksumWriterOptions);

	public bool RequiresQuery => false;

	public bool CanHandle(string connectionString)
	{
		return connectionString.EndsWith(".sha256", StringComparison.OrdinalIgnoreCase);
	}

	public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
	{
		var chkOptions = (ChecksumWriterOptions)options;
		chkOptions.OutputPath = connectionString;

		var logger = serviceProvider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ChecksumDataWriter>>();
		return new ChecksumDataWriter(connectionString, chkOptions, logger);
	}
}
