namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Single definition of internal channel naming conventions.
/// </summary>
public interface IChannelNaming
{
    /// <summary>Prefix of fan-out sub-channel aliases: source "s" fans out to "s__fan_0", "s__fan_1", …</summary>
    const string FanPrefix = "__fan_";
}
