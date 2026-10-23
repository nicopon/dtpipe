using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.SqlServer;

[Description("Reads data from a SQL Server database.")]
[ComponentHelp(
    usageNotes: "Connection string (minimum keys, not exhaustive): 'mssql:Server=host;Database=db;User Id=user;Password=pass;TrustServerCertificate=True'. Driver: Microsoft.Data.SqlClient — its option set defines the full key vocabulary. In YAML, use 'provider-options' -> 'mssql' to specify reader configurations like query or table.",
    examples: new[] {
        "main:\n  input: \"mssql:Server=.;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=True\"\n  provider-options:\n    mssql:\n      query: \"SELECT * FROM dbo.Orders\"\n  output: \"<adapter-prefix>:<target>\""
    })]
public class SqlServerReaderOptions : QueryableReaderOptions, IProviderOptions
{
    public static string Prefix => SqlServerConstants.ProviderName;
    public static string DisplayName => "SQL Server Reader";
}
