using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace DtPipe.Cli.Security;

public class FileSecretsManager : ISecretsManager
{
	private readonly string _filePath;
	private readonly ILogger<FileSecretsManager> _logger;

	public FileSecretsManager(string filePath, ILogger<FileSecretsManager> logger)
	{
		_filePath = filePath;
		_logger = logger;
		_logger.LogWarning("DTPIPE is running with an unsafe file-based keyring fallback. Secrets are stored in PLAIN TEXT at {FilePath}.", filePath);
	}

	public void SetSecret(string alias, string value)
	{
		var secrets = LoadSecrets();
		secrets[alias] = value;
		SaveSecrets(secrets);
	}

	public string? GetSecret(string alias)
	{
		var secrets = LoadSecrets();
		return secrets.TryGetValue(alias, out var val) ? val : null;
	}

	public void DeleteSecret(string alias)
	{
		var secrets = LoadSecrets();
		if (secrets.Remove(alias))
		{
			SaveSecrets(secrets);
		}
	}

	public Dictionary<string, string> ListSecrets()
	{
		return LoadSecrets();
	}

	public void Nuke()
	{
		try
		{
			if (File.Exists(_filePath))
			{
				File.Delete(_filePath);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to delete fake keyring file.");
		}
	}

	private Dictionary<string, string> LoadSecrets()
	{
		try
		{
			if (!File.Exists(_filePath)) return new Dictionary<string, string>();
			var json = File.ReadAllText(_filePath);
			if (string.IsNullOrWhiteSpace(json)) return new Dictionary<string, string>();
			return JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to load fake keyring secrets.");
			return new Dictionary<string, string>();
		}
	}

	private void SaveSecrets(Dictionary<string, string> secrets)
	{
		try
		{
			var dir = Path.GetDirectoryName(_filePath);
			if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
			{
				Directory.CreateDirectory(dir);
				if (!OperatingSystem.IsWindows())
				{
					try
					{
						File.SetUnixFileMode(dir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
					}
					catch
					{
						// Ignore
					}
				}
			}

			var json = JsonSerializer.Serialize(secrets, new JsonSerializerOptions { WriteIndented = true });
			File.WriteAllText(_filePath, json);

			if (!OperatingSystem.IsWindows())
			{
				try
				{
					File.SetUnixFileMode(_filePath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
				}
				catch
				{
					// Ignore
				}
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to save fake keyring secrets.");
		}
	}
}
