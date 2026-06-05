using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Tests.Helpers;

public static class OracleSchemaHelper
{
    private const string TestUser = "testuser";
    private const string TestUserPassword = "password";

    // Resets the test schema to a clean state by dropping and recreating the user.
    // Must be called with an admin connection string (SYSTEM).
    public static async Task ResetSchemaAsync(string adminConnectionString)
    {
        // Close all pooled .NET connections to testuser before dropping the schema,
        // otherwise Oracle raises ORA-01940 (cannot drop a currently connected user).
        OracleConnection.ClearAllPools();

        await using var connection = new OracleConnection(adminConnectionString);
        await connection.OpenAsync();

        // Kill any residual Oracle sessions (e.g. from a previous test run that crashed).
        await ExecuteAsync(connection, $"""
            BEGIN
              FOR s IN (SELECT sid, serial# FROM v$session WHERE username = UPPER('{TestUser}')) LOOP
                BEGIN
                  EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || s.sid || ',' || s.serial# || ''' IMMEDIATE';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
              END LOOP;
            END;
            """);

        await ExecuteAsync(connection, $"DROP USER {TestUser} CASCADE");
        await ExecuteAsync(connection, $"CREATE USER {TestUser} IDENTIFIED BY {TestUserPassword}");
        await ExecuteAsync(connection, $"GRANT CONNECT, RESOURCE TO {TestUser}");
        await ExecuteAsync(connection, $"GRANT UNLIMITED TABLESPACE TO {TestUser}");
    }

    private static async Task ExecuteAsync(OracleConnection connection, string sql)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
