using System;

namespace DtPipe.Adapters.Oracle;

public static class OracleConnectionHelper
{
	public static bool CanHandle(string connectionString)
	{
		if (string.IsNullOrWhiteSpace(connectionString)) return false;

		// Simple heuristic: Contains "Data Source="
		// Prefixes are not checked here; that is handled by JobService.
		return connectionString.Contains("Data Source=", StringComparison.OrdinalIgnoreCase);
	}

	public static string GetConnectionString(string original)
	{
		return original; // No prefix stripping needed anymore
	}
}
