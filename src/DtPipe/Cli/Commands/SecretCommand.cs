using System.CommandLine;
using DtPipe.Cli.Security;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class SecretCommand : Command
{
	public SecretCommand() : base("secret", "Manage secure connection strings in OS Keyring")
	{
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
				AnsiConsole.MarkupLine($"[green]Secret '{alias}' stored successfully.[/]");
			}
			catch (Exception ex)
			{
				AnsiConsole.WriteException(ex);
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
					AnsiConsole.MarkupLine($"[red]Secret '{alias}' not found.[/]");
				else
					Console.WriteLine(secret);
			}
			catch (Exception ex)
			{
				AnsiConsole.WriteException(ex);
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
					AnsiConsole.MarkupLine("[yellow]No secrets found in keyring.[/]");
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

				AnsiConsole.Write(table);
			}
			catch (Exception ex)
			{
				AnsiConsole.WriteException(ex);
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
				AnsiConsole.MarkupLine($"[green]Secret '{alias}' deleted (if it existed).[/]");
			}
			catch (Exception ex)
			{
				AnsiConsole.WriteException(ex);
			}
		});
		return cmd;
	}

	private Command CreateNukeCommand()
	{
		var cmd = new Command("nuke", "Delete ALL secrets (removes container)");
		cmd.SetAction((parseResult) =>
		{
			if (!AnsiConsole.Confirm("Are you sure you want to delete ALL secrets?", false)) return;
			try
			{
				var mgr = new SecretsManager();
				mgr.Nuke();
				AnsiConsole.MarkupLine("[green]All secrets nuked.[/]");
			}
			catch (Exception ex)
			{
				AnsiConsole.WriteException(ex);
			}
		});
		return cmd;
	}
}
