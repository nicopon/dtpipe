using System.Text.Json;
using System.Text.Json.Serialization;

namespace DtPipe.Coordinator.Server;

/// <summary>Where a deployment stands. Its word is the lowercase name (<see cref="StateWords.Word(DeploymentState)"/>).</summary>
public enum DeploymentState { Deploying, Ready, Failed }

/// <summary>Where a run stands. Its word is the lowercase name, written by the converter on the property that carries it.</summary>
public enum RunState { Queued, Running, Succeeded, Failed, Cancelled, Refused, Error }

/// <summary>
/// Writes an enum as its camel-case name, one lowercase word for the states above, and refuses a number. It sits on the
/// property that carries a state: the converter the server adds to every enum writes a name as it is declared, and wins over
/// one placed on the type.
/// </summary>
public sealed class WireEnumConverter<T>() : JsonStringEnumConverter<T>(JsonNamingPolicy.CamelCase, allowIntegerValues: false)
    where T : struct, Enum;

/// <summary>The word of a deployment's state where no property carries it: an event built on the fly, a message. The same policy as <see cref="WireEnumConverter{T}"/>.</summary>
public static class StateWords
{
    public static string Word(this DeploymentState state) => JsonNamingPolicy.CamelCase.ConvertName(state.ToString());

    public static string Word(this RunState state) => JsonNamingPolicy.CamelCase.ConvertName(state.ToString());
}
