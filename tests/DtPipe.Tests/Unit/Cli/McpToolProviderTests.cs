using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using ModelContextProtocol.Server;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class TestTools
{
    [McpServerTool(Name = "hello")]
    [System.ComponentModel.Description("Say hello")]
    public string SayHello(
        [System.ComponentModel.Description("Name to greet")] string name)
    {
        return JsonSerializer.Serialize(new { greeting = $"Hello, {name}!" });
    }
}

public class McpToolProviderTests
{
    [Fact]
    public void McpToolProvider_ExposesToolDefinitions()
    {
        var provider = new McpToolProvider(new TestTools());
        var tools = provider.GetToolDefinitions();

        var helloTool = Assert.Single(tools, t => t.Name == "hello");
        Assert.Equal("Say hello", helloTool.Description);

        // 'ask-user' is always appended — a turn terminator the model can reach in every mode.
        Assert.Contains(tools, t => t.Name == "ask-user");
    }

    [Fact]
    public async Task McpToolProvider_InvokesToolCorrectly()
    {
        var provider = new McpToolProvider(new TestTools());
        using var argsDoc = JsonDocument.Parse(@"{ ""name"": ""World"" }");

        var result = await provider.InvokeToolAsync("hello", argsDoc.RootElement, CancellationToken.None);

        Assert.False(result.IsError);
        Assert.Contains("Hello, World!", result.Content);
    }
}
