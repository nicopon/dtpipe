using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Oracle;

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

public record OracleWriterOptions : IProviderOptions, IKeyAwareOptions
{
	public static string Prefix => OracleConstants.ProviderName;
	public static string DisplayName => "Oracle Writer Options";
	public string? Key { get; set; }

	[CliOption(Description = "Mapping for DateTime columns (Date, Timestamp)", Hidden = true)]
	public OracleDateTimeMapping DateTimeMapping { get; init; } = OracleDateTimeMapping.Date;

	[CliOption(Description = "Target table name", Hidden = true)]
	public string Table { get; set; } = "export";

	[CliOption(Description = "Data write strategy (Append, Truncate, DeleteThenInsert)", Hidden = true)]
	public OracleWriteStrategy? Strategy { get; set; }

	[CliOption(Description = "Data insert mode (Standard, Bulk, Append)", Hidden = true)]
	public OracleInsertMode? InsertMode { get; set; }


}
