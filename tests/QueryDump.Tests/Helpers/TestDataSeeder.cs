using System.Data;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Reflection;

namespace QueryDump.Tests.Helpers;

public record TestRecord(
    int Id, 
    string Name, 
    int Age, 
    bool IsActive, 
    decimal Score, 
    DateTime CreatedAt, 
    DateTime BirthDate
);

public static class TestDataSeeder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public static async Task SeedAsync(IDbConnection connection, string tableName)
    {
        var json = await File.ReadAllTextAsync(Path.Combine("Resources", "test-data.json"));
        var records = JsonSerializer.Deserialize<List<TestRecord>>(json, JsonOptions) 
                      ?? throw new InvalidOperationException("Failed to load test data.");

        foreach (var record in records)
        {
            using var command = connection.CreateCommand();
            var parameters = new Dictionary<string, object>
            {
                { "Id", record.Id },
                { "Name", record.Name },
                { "Age", record.Age },
                { "IsActive", record.IsActive },
                { "Score", record.Score },
                { "CreatedAt", record.CreatedAt },
                { "BirthDate", record.BirthDate }
            };

            command.CommandText = GenerateInsertSql(connection, tableName, parameters);
            AddParameters(command, parameters);

            if (connection.State != ConnectionState.Open)
            {
                // Should already be open, but safe check
                connection.Open();
            }
            
            command.ExecuteNonQuery();
        }
    }

    public static string GenerateTableDDL(IDbConnection connection, string tableName)
    {
        var provider = GetProviderName(connection);

        return provider switch
        {
            "Oracle" => $@"
                CREATE TABLE {tableName} (
                    Id NUMBER PRIMARY KEY,
                    Name VARCHAR2(100),
                    Age NUMBER(3),
                    IsActive NUMBER(1),
                    Score NUMBER(10,2),
                    CreatedAt TIMESTAMP,
                    BirthDate DATE
                )",
            "SqlServer" => $@"
                CREATE TABLE {tableName} (
                    Id INT PRIMARY KEY,
                    Name NVARCHAR(100),
                    Age INT,
                    IsActive BIT,
                    Score DECIMAL(10,2),
                    CreatedAt DATETIME2,
                    BirthDate DATE
                )",
            "DuckDB" => $@"
                CREATE TABLE {tableName} (
                    Id INTEGER PRIMARY KEY,
                    Name VARCHAR,
                    Age INTEGER,
                    IsActive BOOLEAN,
                    Score DECIMAL(10,2),
                    CreatedAt TIMESTAMP,
                    BirthDate DATE
                )",
            "PostgreSQL" => $@"
                CREATE TABLE {tableName} (
                    Id INTEGER PRIMARY KEY,
                    Name VARCHAR(100),
                    Age INTEGER,
                    IsActive BOOLEAN,
                    Score DECIMAL(10,2),
                    CreatedAt TIMESTAMP,
                    BirthDate DATE
                )",
            _ => throw new NotSupportedException($"Provider {provider} not supported for DDL generation.")
        };
    }

    private static string GenerateInsertSql(IDbConnection connection, string tableName, Dictionary<string, object> parameters)
    {
        var provider = GetProviderName(connection);
        var paramPrefix = provider switch
        {
            "Oracle" => ":",
            "DuckDB" => "$",
            "PostgreSQL" => "@",
            _ => "@"
        };
        
        var columns = string.Join(", ", parameters.Keys);
        // Ensure keys are wrapped if needed, but simple names are fine
        var values = string.Join(", ", parameters.Keys.Select(k => $"{paramPrefix}{k}"));

        return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
    }

    private static void AddParameters(IDbCommand command, Dictionary<string, object> parameters)
    {
        var provider = GetProviderName(command.Connection!);

        foreach (var (key, value) in parameters)
        {
            var param = command.CreateParameter();
            param.ParameterName = key; // Most providers handle @/: automatically if just name provided, but explicitly:
            
            // Standardize types if needed
            var finalValue = value;
            if (provider == "Oracle" && value is bool b)
            {
                finalValue = b ? 1 : 0; // Oracle has no BOOLEAN in SQL
            }

            param.Value = finalValue ?? DBNull.Value;
            command.Parameters.Add(param);
        }
    }

    private static string GetProviderName(IDbConnection connection)
    {
        var typeName = connection.GetType().Name;
        if (typeName.Contains("Oracle")) return "Oracle";
        if (typeName.Contains("SqlConnection")) return "SqlServer";
        if (typeName.Contains("DuckDB")) return "DuckDB";
        if (typeName.Contains("Npgsql")) return "PostgreSQL";
        return "Unknown";
    }
}
