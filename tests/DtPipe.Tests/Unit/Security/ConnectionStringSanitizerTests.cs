using DtPipe.Core.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

public class ConnectionStringSanitizerTests
{
	[Theory]
	[InlineData(null, "")]
	[InlineData("", "")]
	[InlineData("   ", "")]
	public void Sanitize_NullOrEmpty_ReturnsEmpty(string? input, string expected)
	{
		var result = ConnectionStringSanitizer.Sanitize(input);
		Assert.Equal(expected, result);
	}

	[Fact]
	public void Sanitize_Keyring_ReturnsAsIs()
	{
		var input = "keyring://my-secret-db";
		var result = ConnectionStringSanitizer.Sanitize(input);
		Assert.Equal(input, result);
	}

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
	{
		var result = ConnectionStringSanitizer.Sanitize(input);
		Assert.Equal(expected, result);
	}

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
	{
		var result = ConnectionStringSanitizer.Sanitize(input);
		Assert.Equal(expected, result);
	}

	[Theory]
	[InlineData("postgresql://postgres@localhost:5432/mydb")]
	[InlineData("sqlite:data.db")]
	[InlineData("csv:/Users/test/file.csv")]
	public void Sanitize_NoCredentials_ReturnsAsIs(string input)
	{
		var result = ConnectionStringSanitizer.Sanitize(input);
		Assert.Equal(input, result);
	}
}
