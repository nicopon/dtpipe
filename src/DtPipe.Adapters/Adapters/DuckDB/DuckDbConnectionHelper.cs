using System;

namespace DtPipe.Adapters.DuckDB;

public static class DuckDbConnectionHelper
{
	public static bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		return connectionString.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase);
	}

	public static string GetConnectionString(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return "Data Source=:memory:;";

		if (!connectionString.Contains('=', StringComparison.OrdinalIgnoreCase))
		{
			return $"Data Source={connectionString};";
		}

		return connectionString;
	}
}
