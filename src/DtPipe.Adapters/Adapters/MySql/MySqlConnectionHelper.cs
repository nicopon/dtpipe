using MySqlConnector;

namespace DtPipe.Adapters.MySql;

public static class MySqlConnectionHelper
{
    /// <summary>
    /// Normalizes a user connection string for the writer path.
    /// <para>
    /// <c>MySqlBulkCopy</c> is implemented on top of LOAD DATA LOCAL INFILE, which the client
    /// refuses to send unless <c>AllowLoadLocalInfile</c> is set. Requiring users to discover and
    /// type that flag to get the bulk path would make the fast route opt-in by accident, so the
    /// writer turns it on for itself. The server side of the same switch (<c>local_infile</c>) is
    /// not ours to set — see <see cref="MySqlDataWriter"/>, which probes it and falls back.
    /// </para>
    /// </summary>
    public static string EnableLocalInfile(string connectionString)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString) { AllowLoadLocalInfile = true };
        return builder.ConnectionString;
    }
}
