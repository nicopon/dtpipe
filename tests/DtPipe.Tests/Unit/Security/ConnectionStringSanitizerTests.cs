using DtPipe.Core.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

public class ConnectionStringSanitizerTests
{
	/// <summary>
	/// Every secret-bearing form this product builds or accepts. A new one is added here first:
	/// the table is the specification, and both entry points are held to it.
	/// </summary>
	public static TheoryData<string, string> SecretForms => new()
	{
		{ "Host=h;Password=SECRET", "SECRET" },
		{ "Server=s;Pwd=SECRET", "SECRET" },
		{ "Token=SECRET;Url=https://api.example.com", "SECRET" },
		{ "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=SECRET==", "SECRET" },
		{ "BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=sv=2021&sig=SECRET", "SECRET" },
		{ "AccessKeyId=AKIA;SecretAccessKey=SECRET", "SECRET" },
		{ "SET s3_secret_access_key='SECRET';", "SECRET" },
		{ "postgresql://postgres:SECRET@localhost:5432/mydb", "SECRET" },
	};

	[Theory]
	[MemberData(nameof(SecretForms))]
	public void Sanitize_Masks_Every_Known_Form(string input, string secret)
		=> Assert.DoesNotContain(secret, ConnectionStringSanitizer.Sanitize(input), System.StringComparison.Ordinal);

	[Theory]
	[MemberData(nameof(SecretForms))]
	public void Redact_Masks_Every_Known_Form(string input, string secret)
		=> Assert.DoesNotContain(secret, ConnectionStringSanitizer.Redact(input), System.StringComparison.Ordinal);

	// ─────────────────────────────────────────────────────────────────────────
	// Redact — fail-closed over a parsed connection string
	// ─────────────────────────────────────────────────────────────────────────

	[Theory]
	[InlineData(
		"Host=localhost;Database=mydb;Username=postgres;Password=mysecret123;",
		"Host=localhost;Database=mydb;Username=postgres;Password=***;")]
	[InlineData(
		"Server=srvA;Database=A;User Id=u;Password=p",
		"Server=srvA;Database=A;User Id=u;Password=***")]
	[InlineData(
		"sqlserver:Server=s;Password=p",
		"sqlserver:Server=s;Password=***")]
	public void Redact_Keeps_Routing_Keys_And_Masks_The_Rest(string input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Redact(input));

	/// <summary>
	/// The property the whitelist buys: a key nobody thought of is masked rather than printed.
	/// A denylist answers the opposite way, which is how four forms shipped in the clear.
	/// </summary>
	[Fact]
	public void Redact_Masks_An_Unknown_Key()
		=> Assert.Equal("Host=h;Quantum=***", ConnectionStringSanitizer.Redact("Host=h;Quantum=tomorrows-secret"));

	/// <summary>
	/// A selector in front of the first key must not hide it. Without stepping over the prefix,
	/// "sqlserver:Password=p" reads as the key "sqlserver:Password", which no rule matches.
	/// </summary>
	[Fact]
	public void Redact_Sees_Through_A_Component_Selector()
		=> Assert.Equal("sqlserver:Password=***", ConnectionStringSanitizer.Redact("sqlserver:Password=p"));

	[Theory]
	[InlineData("csv:/Users/test/file.csv")]
	[InlineData("s3://bucket/key.parquet")]
	[InlineData("duck:memory")]
	[InlineData("sqlite:data.db")]
	[InlineData("keyring://my-secret-db")]
	[InlineData("postgresql://postgres@localhost:5432/mydb")]
	public void Redact_Leaves_A_Location_Alone(string input)
		=> Assert.Equal(input, ConnectionStringSanitizer.Redact(input));

	[Theory]
	[InlineData(null, "")]
	[InlineData("", "")]
	[InlineData("   ", "")]
	public void Redact_NullOrEmpty_ReturnsEmpty(string? input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Redact(input));

	// ─────────────────────────────────────────────────────────────────────────
	// Sanitize — best-effort scan over text with no grammar
	// ─────────────────────────────────────────────────────────────────────────

	[Theory]
	[InlineData(null, "")]
	[InlineData("", "")]
	[InlineData("   ", "")]
	public void Sanitize_NullOrEmpty_ReturnsEmpty(string? input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Sanitize(input));

	[Fact]
	public void Sanitize_Keyring_ReturnsAsIs()
		=> Assert.Equal("keyring://my-secret-db", ConnectionStringSanitizer.Sanitize("keyring://my-secret-db"));

	[Theory]
	[InlineData(
		"Host=localhost;Database=mydb;Username=postgres;Password=mysecret123;",
		"Host=localhost;Database=mydb;Username=postgres;Password=***;")]
	[InlineData(
		"Server=myServerAddress;Database=myDataBase;Uid=myUsername;Pwd=myPassword;",
		"Server=myServerAddress;Database=myDataBase;Uid=myUsername;Pwd=***;")]
	[InlineData(
		"User Id=myUsername;Password=myPassword;Data Source=myOracleDb;",
		"User Id=myUsername;Password=***;Data Source=myOracleDb;")]
	[InlineData(
		"Token=abc-123-xyz;Url=https://api.example.com",
		"Token=***;Url=https://api.example.com")]
	public void Sanitize_StandardConnectionString_MasksPasswords(string input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Sanitize(input));

	/// <summary>
	/// The four forms a word-boundary pattern misses: \b breaks on neither "_" nor a capital,
	/// so there is no boundary around "secret" in "s3_secret_access_key" or "SecretAccessKey".
	/// </summary>
	[Theory]
	[InlineData(
		"DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=Eby8vdM0==",
		"DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=***")]
	[InlineData(
		"BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=sv=2021&sig=abc",
		"BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=***")]
	[InlineData(
		"AccessKeyId=AKIAEXAMPLE;SecretAccessKey=wJalrXUtnFEMI",
		"AccessKeyId=AKIAEXAMPLE;SecretAccessKey=***")]
	[InlineData(
		"LOAD httpfs; SET s3_secret_access_key='wJalrXUtnFEMI'",
		"LOAD httpfs; SET s3_secret_access_key=***")]
	public void Sanitize_Masks_Keys_No_Word_Boundary_Reaches(string input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Sanitize(input));

	[Theory]
	[InlineData(
		"postgresql://postgres:mysecretpassword@localhost:5432/mydb",
		"postgresql://postgres:***@localhost:5432/mydb")]
	[InlineData(
		"mysql://user:pass@127.0.0.1:3306/db?useSSL=false",
		"mysql://user:***@127.0.0.1:3306/db?useSSL=false")]
	[InlineData(
		"http://api_user:api_token_value@api.example.com/v1",
		"http://api_user:***@api.example.com/v1")]
	public void Sanitize_UriCredentials_MasksPasswords(string input, string expected)
		=> Assert.Equal(expected, ConnectionStringSanitizer.Sanitize(input));

	[Theory]
	[InlineData("postgresql://postgres@localhost:5432/mydb")]
	[InlineData("sqlite:data.db")]
	[InlineData("csv:/Users/test/file.csv")]
	[InlineData("Column 'Id' not found in source; expected=3 actual=5")]
	public void Sanitize_NoCredentials_ReturnsAsIs(string input)
		=> Assert.Equal(input, ConnectionStringSanitizer.Sanitize(input));
}
