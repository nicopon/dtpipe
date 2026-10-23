using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.MySql;

[Description("Reads data from a MySQL or MariaDB database.")]
[ComponentHelp(
    usageNotes: "Connection string (minimum keys, not exhaustive): 'mysql:Server=host;Port=3306;Database=db;User ID=user;Password=pass'. The 'mysql:' prefix is required — a MySQL connection string is indistinguishable from a SQL Server one by content alone. Driver: MySqlConnector — its option set defines the full key vocabulary, and is not identical to MySql.Data's. In YAML, use 'provider-options' -> 'mysql' to specify reader configurations like query or table.",
    examples: new[] {
        "main:\n  input: \"mysql:Server=localhost;Database=prod;User ID=root;Password=pass\"\n  provider-options:\n    mysql:\n      query: \"SELECT * FROM orders\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public class MySqlReaderOptions : QueryableReaderOptions, IProviderOptions
{
    public static string Prefix => MySqlConstants.ProviderName;
    public static string DisplayName => "MySQL Reader";
}
