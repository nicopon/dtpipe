using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.PostgreSQL;

[Description("Reads data from a PostgreSQL database.")]
[ComponentHelp(
    usageNotes: "Connection string (minimum keys, not exhaustive): 'pg:Host=host;Port=port;Database=db;Username=user;Password=pass'. Driver: Npgsql — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'pg' to specify reader configurations like query or table.",
    examples: new[] {
        "main:\n  input: \"pg:Host=localhost;Database=prod;Username=postgres\"\n  provider-options:\n    pg:\n      query: \"SELECT * FROM public.orders\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public class PostgreSqlReaderOptions : QueryableReaderOptions, IProviderOptions
{
    public static string Prefix => PostgreSqlConstants.ProviderName;
    public static string DisplayName => "PostgreSQL Reader";
}
