using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.S3;

/// <summary>
/// Credential and endpoint settings shared by the S3 reader and writer. Values flow into a
/// scoped DuckDB secret; leaving the key pair empty falls back to DuckDB's credential chain
/// (AWS_* environment variables, shared config, instance profile).
/// </summary>
public abstract class S3ConnectionOptions
{
    [ComponentOption("--s3-endpoint", Description = "S3-compatible endpoint, e.g. 'http://127.0.0.1:9000' for MinIO. Omit for AWS. An explicit http:// disables TLS and selects path-style addressing.")]
    public string? Endpoint { get; set; }

    [ComponentOption("--s3-region", Description = "Bucket region, e.g. 'eu-west-1'.")]
    public string? Region { get; set; }

    [ComponentOption("--s3-access-key", Description = "Access key id. Omit to use the ambient credential chain (env, shared config, instance profile).")]
    public string? AccessKey { get; set; }

    [ComponentOption("--s3-secret-key", Description = "Secret access key. Use ${{keyring://alias}} rather than a literal.")]
    public string? SecretKey { get; set; }

    [ComponentOption("--s3-session-token", Description = "Session token for temporary credentials.")]
    public string? SessionToken { get; set; }

    [ComponentOption("--s3-url-style", Description = "Addressing style: 'vhost' or 'path'. Defaults to 'path' when --s3-endpoint is set.")]
    public string? UrlStyle { get; set; }
}

[Description("Reads a file from S3-compatible object storage (s3://, s3a://).")]
[ComponentHelp(
	usageNotes: "Connection string is an object URI such as 's3://bucket/prefix/data.parquet'. The format is resolved from the extension (.parquet, .csv, .tsv, .json, .jsonl, .ndjson); globs like 's3://bucket/dt=*/part-*.parquet' are read natively. Credentials are set through --s3-* options or the ambient AWS credential chain. In YAML, use 'provider-options' -> 's3'.",
	examples: new[] {
		"main:\n  input: \"s3://analytics/events/2026-08-*.parquet\"\n  provider-options:\n    s3:\n      region: \"eu-west-1\"\n      secret-key: \"${{keyring://aws-secret}}\"\n  output: \"<adapter-prefix>:<target>\""
	})]
public class S3ReaderOptions : S3ConnectionOptions, IProviderOptions
{
    public static string Prefix => "s3";
    public static string DisplayName => "S3 Reader";
}

[Description("Writes a file to S3-compatible object storage (s3://, s3a://).")]
[ComponentHelp(
	usageNotes: "Connection string is an object URI such as 's3://bucket/prefix/data.parquet'. The format is resolved from the extension. Writing replaces the target key: object storage has no append or upsert, so --strategy does not apply. The upload is issued once the pipeline completes, so a failed run leaves the existing object untouched.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"s3://warehouse/sales/2026-08.parquet\"\n  provider-options:\n    s3:\n      endpoint: \"http://127.0.0.1:9000\"\n      access-key: \"${{keyring://minio-key}}\"\n      secret-key: \"${{keyring://minio-secret}}\""
	})]
public class S3WriterOptions : S3ConnectionOptions, IProviderOptions
{
    public static string Prefix => "s3";
    public static string DisplayName => "S3 Writer";
}
