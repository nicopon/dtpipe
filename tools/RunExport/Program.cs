using System;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Configuration;
using DtPipe.Core.Options;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Csv;
using DtPipe.Transformers.Fake;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Abstractions;
using DtPipe.Feedback;
using DtPipe;
using Spectre.Console;
using System.IO;
using System.Threading.Tasks;

class Program
{
    static async Task<int> Main()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"runexport_{Guid.NewGuid()}.duckdb");
        var connStr = $"Data Source={dbPath}";
        var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "tests", "output");
        Directory.CreateDirectory(outputDir);
        var outputPath = Path.Combine(outputDir, "runexport_out.csv");
        try
        {
            // Create DB and seed
            using (var conn = new DuckDB.NET.Data.DuckDBConnection(connStr))
            {
                await conn.OpenAsync();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE users (Id INTEGER, Name VARCHAR, Email VARCHAR, Age INTEGER);";
                await cmd.ExecuteNonQueryAsync();
                cmd.CommandText = "INSERT INTO users VALUES (1,'Alice','alice@example.com',30),(2,'Bob','bob@example.com',25),(3,'Charlie','charlie@example.com',40),(4,'David','david@example.com',28);";
                await cmd.ExecuteNonQueryAsync();
            }

            var registry = new OptionsRegistry();
            registry.Register(new DuckDbOptions());
            registry.Register(new CsvOptions { Header = true });
            registry.Register(new FakeOptions { Mappings = new[] { "Name:name.firstname" }, Seed = 12345 });

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(registry);

            services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
                new DuckDbReaderDescriptor(), sp.GetRequiredService<OptionsRegistry>(), sp));

            services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
                new DtPipe.Adapters.Csv.CsvWriterDescriptor(), sp.GetRequiredService<OptionsRegistry>(), sp));

            services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
            services.AddSingleton<ExportService>();
            services.AddSingleton<IAnsiConsole>(AnsiConsole.Console);

            var sp = services.BuildServiceProvider();
            var exportService = sp.GetRequiredService<ExportService>();

            var options = new DtPipe.Configuration.DumpOptions
            {
                Provider = "duckdb",
                ConnectionString = connStr,
                Query = "SELECT * FROM users ORDER BY Id",
                OutputPath = outputPath,
                BatchSize = 100
            };

            var transformerFactories = sp.GetServices<IDataTransformerFactory>().ToList();
            var pipelineBuilder = new DtPipe.Core.Pipelines.TransformerPipelineBuilder(transformerFactories);
            var pipeline = pipelineBuilder.Build(new[] { "dtpipe", "--fake", "Name:name.firstname" });

            var readerFactory = sp.GetRequiredService<IStreamReaderFactory>();
            var writerFactory = sp.GetRequiredService<IDataWriterFactory>();

            await exportService.RunExportAsync(options, CancellationToken.None, pipeline, readerFactory, writerFactory);

            Console.WriteLine($"Exported to: {outputPath}");
            var lines = await File.ReadAllLinesAsync(outputPath);
            Console.WriteLine($"Lines: {lines.Length}");
            for (int i = 0; i < lines.Length; i++) Console.WriteLine($"{i}: {lines[i]}");

            return 0;
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch {}
            // keep output for inspection
        }
    }
}
