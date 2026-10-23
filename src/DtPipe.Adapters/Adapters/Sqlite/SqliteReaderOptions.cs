using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sqlite;

[Description("Reads data from an SQLite database.")]
[ComponentHelp(
    usageNotes: "Connection string (minimum keys, not exhaustive): 'sqlite:Data Source=path/to/db.db'. Driver: Microsoft.Data.Sqlite — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'sqlite' to specify reader configurations like query or table.",
    examples: new[] {
        "main:\n  input: \"sqlite:Data Source=business.db\"\n  provider-options:\n    sqlite:\n      query: \"SELECT * FROM company_clients\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public class SqliteReaderOptions : QueryableReaderOptions, IProviderOptions
{
    public static string Prefix => "sqlite";
    public static string DisplayName => "SQLite Reader";
}
