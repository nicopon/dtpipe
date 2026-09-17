using System.ComponentModel;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

[Description("Reads data from an Apache Arrow IPC file or stream.")]
[ComponentHelp(
	usageNotes: "Connection string is a file path ending in '.arrow', '.arrowfile', or '.ipc' (or the 'arrow:' prefix; '-' for stdin). Files named '.arrow'/'.arrowfile' are read with the seekable IPC file reader (random access to any record batch); any other extension, including stdin, uses the sequential IPC stream reader. A stream that stops without the IPC end-of-stream marker is refused rather than read as complete: a producer that dies mid-flow leaves a clean message boundary, which would otherwise be indistinguishable from a finished transfer and would report a partial result as a success.",
	examples: new[] {
		"main:\n  input: \"data.arrow\"\n  output: \"<adapter-prefix>:<target>\""
	})]
public class ArrowReaderOptions : IOptionSet
{
	public static string Prefix => ArrowConstants.ProviderName;
	public static string DisplayName => "Arrow Reader";
}
