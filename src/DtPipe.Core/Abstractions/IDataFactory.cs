namespace DtPipe.Core.Abstractions;

/// <summary>
/// Base factory interface for data components (Readers/Writers) that can be selected via connection string.
/// </summary>
public interface IDataFactory : IComponentDescriptor
{

	/// <summary>
    /// Indicates if this provider supports standard input/output pipes (-).
    /// Default is false.
    /// </summary>
    bool SupportsStdio => false;

	/// <summary>
	/// Determines if this factory can handle the given connection string or file path as a fallback.
	/// <para>
	/// <b>WARNING:</b> This method receives the RAW, unparsed connection string directly from the user
	/// (e.g. <c>--output "dirty:Host=..."</c>).
	/// It is NOT stripped of any prefixes if the prefix didn't match the <see cref="IComponentDescriptor.ComponentName"/>.
	/// Because of this, implementations SHOULD NOT check for their own prefixes here,
	/// as it would lead to passing dirty/unstripped connection strings to ADO.NET providers.
	/// Instead, <c>CanHandle</c> should only test for file extensions (e.g. <c>.csv</c>)
	/// or database-specific keywords (e.g. <c>Data Source=</c>, <c>Host=</c>).
	/// </para>
	/// </summary>
	bool CanHandle(string connectionString);
}
