using System;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class McpToolsTests
{
    private readonly ServiceProvider _serviceProvider;
    private readonly DtPipeMcpTools _tools;

    public McpToolsTests()
    {
        var services = new ServiceCollection();
        services.AddSingleton<DtPipe.Core.Options.OptionsRegistry>();
        _serviceProvider = services.BuildServiceProvider();

        _tools = new DtPipeMcpTools(
            Array.Empty<DtPipe.Core.Abstractions.IStreamReaderFactory>(),
            Array.Empty<DtPipe.Core.Abstractions.IDataTransformerFactory>(),
            Array.Empty<DtPipe.Core.Abstractions.IDataWriterFactory>(),
            _serviceProvider);
    }

    private void InvokeValidatePathSafety(string path)
    {
        var method = typeof(DtPipeMcpTools).GetMethod("ValidatePathSafety", 
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        try
        {
            method.Invoke(null, new object?[] { path });
        }
        catch (TargetInvocationException ex)
        {
            throw ex.InnerException ?? ex;
        }
    }

    private string[] InvokeSplitArguments(string commandLine)
    {
        var method = typeof(DtPipeMcpTools).GetMethod("SplitArguments", 
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return (string[])method.Invoke(null, new object[] { commandLine })!;
    }

    [Fact]
    public void ValidatePathSafety_PathWithinCwd_Success()
    {
        var relativePath = "data.csv";
        var nestedPath = Path.Combine("subfolder", "data.parquet");
        var absoluteInCwd = Path.Combine(Directory.GetCurrentDirectory(), "data.jsonl");

        // Should not throw
        InvokeValidatePathSafety(relativePath);
        InvokeValidatePathSafety(nestedPath);
        InvokeValidatePathSafety(absoluteInCwd);
    }

    [Fact]
    public void ValidatePathSafety_PathOutsideCwd_Throws()
    {
        var absoluteOutside = "/etc/passwd";
        var relativeParentEscaped = "../outside_cwd.csv";
        var complexEscaped = "subfolder/../../outside.csv";

        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(absoluteOutside));
        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(relativeParentEscaped));
        Assert.Throws<UnauthorizedAccessException>(() => InvokeValidatePathSafety(complexEscaped));
    }

    [Theory]
    [InlineData("Host=localhost;Database=mydb;Username=postgres;Password=123;")]
    [InlineData("Server=myServer;Database=db;User Id=uid;Password=pwd;")]
    [InlineData("sqlite:Host=dummy;Database=ignored;")]
    [InlineData(":memory:")]
    [InlineData("-")]
    public void ValidatePathSafety_DbConnectionStringOrSpecial_SkipsCheck(string path)
    {
        // Should not throw even though it doesn't represent a valid file path inside CWD
        InvokeValidatePathSafety(path);
    }


    [Fact]
    public void SplitArguments_SimpleAndQuotes_ParsedCorrectly()
    {
        var command = "dtpipe -i file.csv --sql \"SELECT * FROM table\" -o out.parquet";
        var expected = new[] { "dtpipe", "-i", "file.csv", "--sql", "SELECT * FROM table", "-o", "out.parquet" };

        var result = InvokeSplitArguments(command);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void McpSecurityContext_StateChange_Works()
    {
        Assert.False(McpSecurityContext.IsMcpSession);
        
        McpSecurityContext.IsMcpSession = true;
        Assert.True(McpSecurityContext.IsMcpSession);
        
        McpSecurityContext.IsMcpSession = false;
        Assert.False(McpSecurityContext.IsMcpSession);
    }
}
