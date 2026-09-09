using System;
using System.Reflection;
using DtPipe.Cli.Agent;
using OpenAI;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <c>--llm-timeout</c> means one thing across providers: the longest silence tolerated from the
/// model, never a budget for its answer. The Ollama client carried the same two defects this file
/// pins on the OpenAI one — a transport deadline of its own that quietly overrode the flag, and a
/// blocking path that capped total call duration while the streaming path reset on every update.
/// </summary>
public class OpenAiClientTimeoutTests
{
    /// <summary>
    /// The SDK pipeline's NetworkTimeout defaults to 100 s and whichever deadline expires first
    /// wins, so leaving it unset makes the flag inert above 100 s while the error still reports the
    /// flag's value.
    /// </summary>
    [Fact]
    public void The_Client_Imposes_No_Deadline_Of_Its_Own()
    {
        var options = OpenAiClient.BuildClientOptions("http://localhost:1234");

        Assert.Equal(System.Threading.Timeout.InfiniteTimeSpan, options.NetworkTimeout);
        Assert.Equal(new Uri("http://localhost:1234/v1"), options.Endpoint);
    }

    /// <summary>
    /// The blocking entry point reads the same streamed completion, so there is one deadline
    /// semantics rather than one per rendering mode. Delegation is what makes that true; a
    /// re-implemented blocking call is how the two drifted apart in the first place.
    /// </summary>
    [Fact]
    public void The_Blocking_Call_Delegates_To_The_Streaming_One()
    {
        var chat = typeof(OpenAiClient).GetMethod(nameof(OpenAiClient.ChatAsync));
        Assert.NotNull(chat);

        // An expression-bodied delegation is not async: the compiler emits no state machine.
        Assert.Null(chat.GetCustomAttribute<System.Runtime.CompilerServices.AsyncStateMachineAttribute>());
    }
}
