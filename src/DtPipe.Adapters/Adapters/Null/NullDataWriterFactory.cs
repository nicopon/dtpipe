using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Adapters.Null;

public class NullDataWriterFactory : IProviderDescriptor<IDataWriter>
{
    public string ComponentName => "null";
    public string Category => "Writers";
    public Type OptionsType => typeof(NullDataWriterOptions);

    public bool CanHandle(string connectionString) =>
        connectionString.StartsWith("null:", StringComparison.OrdinalIgnoreCase) ||
        connectionString == "-";

    public bool RequiresQuery => false;

    public IDataWriter Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        return new NullDataWriter();
    }
}

public class NullDataWriterOptions : IWriterOptions
{
    public static string Prefix => "null";
    public static string DisplayName => "Null Data Writer";
}
