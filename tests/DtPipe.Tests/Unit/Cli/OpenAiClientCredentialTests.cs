using DtPipe.Cli.Agent;
using DtPipe.Cli.Studio;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <c>--llm-key</c> is optional: a local OpenAI-compatible server that does not authenticate is a
/// valid target, so the chat client builds without a key rather than refusing the empty string.
/// </summary>
[Collection("console-serial")]
public class OpenAiClientCredentialTests
{
    [Theory]
    [InlineData("")]
    [InlineData("sk-test")]
    public void The_Chat_Client_Builds_With_Or_Without_A_Key(string key)
    {
        var client = new OpenAiClient(key);

        var chat = Record.Exception(() => client.BuildClient("http://127.0.0.1:8000", "any-model"));

        Assert.Null(chat);
    }

    [Fact]
    public void Studio_OpenAi_Client_Without_Key_Ignores_Environment_Fallback()
    {
        var previous = Environment.GetEnvironmentVariable("DTPIPE_LLM_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_LLM_API_KEY", "sk-env-secret");
            var client = Assert.IsType<OpenAiClient>(StudioAgentClients.Create("openai", null, TimeSpan.FromSeconds(10)));
            Assert.False(client.HasApiKey);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_LLM_API_KEY", previous);
        }
    }
}
