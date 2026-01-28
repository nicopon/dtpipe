using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

namespace QueryDump.Adapters.Oracle;

public class OracleWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => OracleConstants.ProviderName;

    public Type OptionsType => typeof(OracleWriterOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return OracleConnectionHelper.CanHandle(connectionString);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        return new OracleDataWriter(connectionString, (OracleWriterOptions)options, loggerFactory.CreateLogger<OracleDataWriter>());
    }
}
