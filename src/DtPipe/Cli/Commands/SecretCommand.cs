using System.CommandLine;
using DtPipe.Cli.Security;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class SecretCommand : Command
{
	private readonly IAnsiConsole _console;

	public SecretCommand(IAnsiConsole console) : base("secret", "Manage secure connection strings in OS Keyring")
	{
		_console = console;
		Subcommands.Add(CreateSetCommand());
		Subcommands.Add(CreateGetCommand());
		Subcommands.Add(CreateListCommand());
		Subcommands.Add(CreateDeleteCommand());
		Subcommands.Add(CreateNukeCommand());
	}

	private Command CreateSetCommand()
	{
		var cmd = new Command("set", "Store a secret");
		var aliasArg = new Argument<string>("alias") { Description = "Name of the secret (e.g. prod-db)" };
		var valueArg = new Argument<string>("value") { Description = "Connection string or secret value" };

		cmd.Arguments.Add(aliasArg);
		cmd.Arguments.Add(valueArg);

		cmd.SetAction((parseResult) =>
		{
			var alias = parseResult.GetValue(aliasArg);
			var value = parseResult.GetValue(valueArg);
			if (string.IsNullOrEmpty(alias) || string.IsNullOrEmpty(value)) return;

			try
			{
				var mgr = new SecretsManager();
				mgr.SetSecret(alias, value);
				_console.MarkupLine($"[green]Secret '{alias}' stored successfully.[/]");
			}
			catch (Exception ex)
			{
				_console.WriteException(ex);
			}
		});

		return cmd;
	}

	private Command CreateGetCommand()
	{
		var cmd = new Command("get", "Retrieve a secret (for verification)");
		var aliasArg = new Argument<string>("alias") { Description = "Name of the secret" };
		cmd.Arguments.Add(aliasArg);

		cmd.SetAction((parseResult) =>
		{
			var alias = parseResult.GetValue(aliasArg);
			if (string.IsNullOrEmpty(alias)) return;

			try
			{
				var mgr = new SecretsManager();
				var secret = mgr.GetSecret(alias);
				if (secret == null)
					_console.MarkupLine($"[red]Secret '{alias}' not found.[/]");
				else
					_console.WriteLine(secret); // Secret content can be written to STDERR or STDOUT?
					// Wait, if it's 'get', maybe user wants it on STDOUT to pipe it?
					// But dtpipe secrets are usually for internal use.
					// User's request: "garantir que cette manipulation de la console est paramétrée pour écrire sur STDERR"
					// I'll stick to _console (STDERR).
			}
			catch (Exception ex)
			{
				_console.WriteException(ex);
			}
		});

		return cmd;
	}

	private Command CreateListCommand()
	{
		var cmd = new Command("list", "List all stored secret aliases");
		cmd.SetAction((parseResult) =>
		{
			try
			{
				var mgr = new SecretsManager();
				var secrets = mgr.ListSecrets();
				if (secrets.Count == 0)
				{
					_console.MarkupLine("[yellow]No secrets found in keyring.[/]");
					return;
				}

				var table = new Table();
				table.AddColumn("Alias");
				table.AddColumn("Preview (First 10 chars)");

				foreach (var kvp in secrets)
				{
					var preview = kvp.Value.Length > 10 ? kvp.Value.Substring(0, 10) + "..." : kvp.Value;
					table.AddRow(kvp.Key, $"[grey]{preview}[/]");
				}

				_console.Write(table);
			}
			catch (Exception ex)
			{
				_console.WriteException(ex);
			}
		});
		return cmd;
	}

	private Command CreateDeleteCommand()
	{
		var cmd = new Command("delete", "Delete a specific secret");
		var aliasArg = new Argument<string>("alias") { Description = "Name of the secret" };
		cmd.Arguments.Add(aliasArg);

		cmd.SetAction((parseResult) =>
		{
			var alias = parseResult.GetValue(aliasArg);
			if (string.IsNullOrEmpty(alias)) return;

			try
			{
				var mgr = new SecretsManager();
				mgr.DeleteSecret(alias);
				_console.MarkupLine($"[green]Secret '{alias}' deleted (if it existed).[/]");
			}
			catch (Exception ex)
			{
				_console.WriteException(ex);
			}
		});
		return cmd;
	}

	private Command CreateNukeCommand()
	{
		var cmd = new Command("nuke", "Delete ALL secrets (removes container)");
		cmd.SetAction((parseResult) =>
		{
			if (!_console.Confirm("Are you sure you want to delete ALL secrets?", false)) return;
			try
			{
				var mgr = new SecretsManager();
				mgr.Nuke();
				_console.MarkupLine("[green]All secrets nuked.[/]");
			}
			catch (Exception ex)
			{
				_console.WriteException(ex);
			}
		});
		return cmd;
	}
}
