using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Oracle;

public class OracleWriterOptions : DbWriterOptions, IProviderOptions
{
	public static string Prefix => OracleConstants.ProviderName;
	public static string DisplayName => "Oracle Writer Options";

	[ComponentOption(Description = "Mapping for DateTime columns (Date, Timestamp)", Hidden = true)]
	public OracleDateTimeMapping DateTimeMapping { get; set; } = OracleDateTimeMapping.Date;

	[ComponentOption("--table", Aliases = new[] { "-t" }, Description = "Target table name", Required = true)]
	public string Table { get; set; } = string.Empty;

	[ComponentOption("--strategy", Aliases = new[] { "-s" }, Description = "Data write strategy (Append, Truncate, DeleteThenInsert)", Hidden = true)]
	public OracleWriteStrategy? Strategy { get; set; }

	[ComponentOption("--insert-mode", Description = "Data insert mode (Standard, Bulk, Append)", Hidden = true)]
	public OracleInsertMode? InsertMode { get; set; }
}

public enum OracleWriteStrategy
{
	Append,
	Truncate,
	DeleteThenInsert,
	Recreate,
	Upsert,
	Ignore
}

public enum OracleInsertMode
{
	Standard,
	Bulk,
	Append
}

public enum OracleDateTimeMapping
{
	Date,
	Timestamp
}
