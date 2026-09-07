using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Azure;

/// <summary>
/// Credential settings shared by the Azure Blob reader and writer. Exactly one of the
/// connection string, the SAS token, or the account name/key pair is needed; with none of them
/// set, DuckDB's Azure credential chain is used.
/// </summary>
public abstract class AzureConnectionOptions
{
    [ComponentOption("--azure-connection-string", Description = "Full storage connection string. Use ${{keyring://alias}} rather than a literal.")]
    public string? ConnectionString { get; set; }

    [ComponentOption("--azure-account-name", Description = "Storage account name.")]
    public string? AccountName { get; set; }

    [ComponentOption("--azure-account-key", Description = "Storage account key. Use ${{keyring://alias}} rather than a literal.")]
    public string? AccountKey { get; set; }

    [ComponentOption("--azure-sas", Description = "Shared access signature token, with or without the leading '?'.")]
    public string? SasToken { get; set; }

    [ComponentOption("--azure-endpoint", Description = "Blob endpoint override, e.g. 'http://127.0.0.1:10000/devstoreaccount1' for Azurite.")]
    public string? Endpoint { get; set; }
}

[Description("Reads a file from Azure Blob Storage (azure://, az://).")]
[ComponentHelp(
	usageNotes: "Connection string is an object URI such as 'azure://container/prefix/data.parquet'. The format is resolved from the extension (.parquet, .csv, .tsv, .json, .jsonl, .ndjson). In YAML, use 'provider-options' -> 'azure'.",
	examples: new[] {
		"main:\n  input: \"azure://reports/daily/2026-08-26.parquet\"\n  provider-options:\n    azure:\n      connection-string: \"${{keyring://azure-conn}}\"\n  output: \"<adapter-prefix>:<target>\""
	})]
public class AzureReaderOptions : AzureConnectionOptions, IProviderOptions
{
    public static string Prefix => "azure";
    public static string DisplayName => "Azure Blob Reader";
}

[Description("Writes a file to Azure Blob Storage (azure://, az://).")]
[ComponentHelp(
	usageNotes: "Connection string is an object URI such as 'azure://container/prefix/data.parquet'. Writing replaces the target blob: object storage has no append or upsert, so --strategy does not apply.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"azure://reports/daily/2026-08-26.parquet\"\n  provider-options:\n    azure:\n      connection-string: \"${{keyring://azure-conn}}\""
	})]
public class AzureWriterOptions : AzureConnectionOptions, IProviderOptions
{
    public static string Prefix => "azure";
    public static string DisplayName => "Azure Blob Writer";
}
