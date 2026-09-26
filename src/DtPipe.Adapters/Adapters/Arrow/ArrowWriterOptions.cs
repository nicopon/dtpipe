using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

[Description("Writes data to an Apache Arrow IPC file or stream.")]
[ComponentHelp(
	usageNotes: "Connection string is a file path ending in '.arrow' or '.arrowfile' (or the 'arrow:' prefix; '-' for stdout; 'pipe://<name>' to connect to a named pipe as a client - something else must already be serving it, such as a distributed pipeline node), which selects the seekable IPC file writer; any other form, including stdout and a named pipe, uses the sequential IPC stream writer instead.",
	examples: new[] {
		"main:\n  input: \"<adapter-prefix>:<source>\"\n  output: \"data.arrow\""
	})]
public class ArrowWriterOptions : IOptionSet
{
	public static string Prefix => ArrowConstants.ProviderName;
	public static string DisplayName => "Arrow Writer";

    [Description("Internal buffer size for column batching")]
    public int BatchSize { get; set; } = 10000;
}
