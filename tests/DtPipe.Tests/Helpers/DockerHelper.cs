using System.Diagnostics;

namespace DtPipe.Tests.Helpers;

public static class DockerHelper
{
	private static bool? _isAvailable;

	public static bool IsAvailable()
	{
		if (_isAvailable.HasValue) return _isAvailable.Value;

		try
		{
			using var process = new Process
			{
				StartInfo = new ProcessStartInfo
				{
					FileName = "docker",
					Arguments = "info",
					RedirectStandardOutput = true,
					RedirectStandardError = true,
					UseShellExecute = false,
					CreateNoWindow = true
				}
			};

			process.Start();
			process.WaitForExit(3000); // Allow bit more time for info

			_isAvailable = process.ExitCode == 0;
		}
		catch
		{
			_isAvailable = false;
		}

		return _isAvailable.Value;
	}

	public static bool ShouldReuseInfrastructure()
	{
		var val = Environment.GetEnvironmentVariable("DTPIPE_TEST_REUSE_INFRA");
		return !string.IsNullOrEmpty(val) && (val.ToLower() == "true" || val == "1");
	}

	public const string LocalPostgreSqlConnectionString = "host=localhost;port=5440;database=integration;username=postgres;password=password";
	public const string LocalSqlServerConnectionString = "Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;Encrypt=False;TrustServerCertificate=True;";
	public const string LocalOracleConnectionString = "Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password";

	public static async Task<string> GetPostgreSqlConnectionString(Func<Task<string>> containerStartFunc)
	{
		if (ShouldReuseInfrastructure())
		{
			return LocalPostgreSqlConnectionString;
		}
		return await containerStartFunc();
	}

	public static async Task<string> GetSqlServerConnectionString(Func<Task<string>> containerStartFunc)
	{
		if (ShouldReuseInfrastructure())
		{
			return LocalSqlServerConnectionString;
		}
		return await containerStartFunc();
	}

	public static async Task<string> GetOracleConnectionString(Func<Task<string>> containerStartFunc)
	{
		if (ShouldReuseInfrastructure())
		{
			return LocalOracleConnectionString;
		}
		return await containerStartFunc();
	}
}


