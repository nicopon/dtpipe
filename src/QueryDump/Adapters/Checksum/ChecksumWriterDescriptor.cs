using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace QueryDump.Adapters.Checksum;

public class ChecksumWriterDescriptor : IProviderDescriptor<IDataWriter>
{
    public string ProviderName => "checksum";
    public Type OptionsType => typeof(ChecksumOptions);

    public bool CanHandle(string connectionString)
    {
        return connectionString.StartsWith("checksum:", StringComparison.OrdinalIgnoreCase) 
               || connectionString.EndsWith(".hash", StringComparison.OrdinalIgnoreCase)
               || connectionString.EndsWith(".sha256", StringComparison.OrdinalIgnoreCase);
    }

    public IDataWriter Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var chkOptions = (ChecksumOptions)options;
        
        // Handle prefix stripping logic similar to other adapters
        if (connectionString.StartsWith("checksum:", StringComparison.OrdinalIgnoreCase))
        {
            chkOptions.OutputPath = connectionString.Substring(9);
        }
        else
        {
            chkOptions.OutputPath = connectionString;
        }

        return new ChecksumWriter(chkOptions, 
            serviceProvider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ChecksumWriter>>());
    }
}
