using AwesomeAssertions;
using DtPipe.Cli.Infrastructure;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// A local path with a wildcard used to reach the adapter intact and raise FileNotFoundException
/// on the pattern itself, while <c>s3://</c> and <c>azure://</c> globbed — so a directory of daily
/// files needed a shell loop on local disk and none in object storage.
/// </summary>
public class LocalFileGlobTests : IDisposable
{
	private readonly string _dir = Path.Combine(Path.GetTempPath(), $"glob_{Guid.NewGuid():N}");

	public LocalFileGlobTests()
	{
		Directory.CreateDirectory(_dir);
		foreach (var name in new[] { "b.csv", "a.csv", "c.csv", "notes.txt" })
			File.WriteAllText(Path.Combine(_dir, name), "id\n1\n");
	}

	public void Dispose()
	{
		if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
		GC.SuppressFinalize(this);
	}

	[Fact]
	public void A_wildcard_in_the_file_name_is_a_pattern()
		=> LocalFileGlob.IsPattern("/data/daily/*.csv").Should().BeTrue();

	[Fact]
	public void A_question_mark_is_a_pattern()
		=> LocalFileGlob.IsPattern("/data/part-?.csv").Should().BeTrue();

	[Fact]
	public void A_plain_path_is_not_a_pattern()
		=> LocalFileGlob.IsPattern("/data/daily/events.csv").Should().BeFalse();

	[Fact]
	public void A_remote_uri_is_never_a_pattern()
	{
		LocalFileGlob.IsPattern("s3://bucket/dt=*/part-*.parquet").Should().BeFalse(
			"the scheme's own provider globs it");
		LocalFileGlob.IsPattern("azure://reports/2026-*.parquet").Should().BeFalse();
	}

	[Fact]
	public void An_empty_input_is_not_a_pattern()
		=> LocalFileGlob.IsPattern("").Should().BeFalse();

	[Fact]
	public void Matches_come_back_in_ordinal_order()
		=> LocalFileGlob.Expand(Path.Combine(_dir, "*.csv"))
			.Select(Path.GetFileName)
			.Should().Equal("a.csv", "b.csv", "c.csv");

	[Fact]
	public void The_pattern_selects_by_extension()
		=> LocalFileGlob.Expand(Path.Combine(_dir, "*.txt"))
			.Select(Path.GetFileName)
			.Should().Equal("notes.txt");

	[Fact]
	public void Matching_nothing_is_an_error_rather_than_an_empty_read()
	{
		var act = () => LocalFileGlob.Expand(Path.Combine(_dir, "*.parquet"));
		act.Should().Throw<FileNotFoundException>().WithMessage("*No file matches*");
	}

	[Fact]
	public void A_missing_directory_says_so()
	{
		var act = () => LocalFileGlob.Expand(Path.Combine(_dir, "absent", "*.csv"));
		act.Should().Throw<DirectoryNotFoundException>();
	}
}
