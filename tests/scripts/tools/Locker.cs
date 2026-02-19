#!/usr/bin/env dotnet
#:property TargetFramework=net10.0
#:package Microsoft.Data.Sqlite@10.0.3
#:package SQLitePCLRaw.bundle_sqlite3@2.1.11

using Microsoft.Data.Sqlite;
using System.Threading;
using SQLitePCL;
using System.IO;
using System;

// Initialize System SQLite provider (avoids looking for e_sqlite3 native lib)
Batteries_V2.Init();

if (args.Length < 2)
{
    Console.WriteLine("Usage: Locker.cs <db_path> <duration_ms>");
    return;
}

string dbPath = args[0];
if (!int.TryParse(args[1], out int durationMs))
{
    Console.WriteLine("Invalid duration");
    return;
}

string signalFile = dbPath + ".signal";
// Ensure clean state at startup
if (File.Exists(signalFile)) File.Delete(signalFile);

Console.WriteLine($"🔒 [Locker] Acquiring EXCLUSIVE lock on {dbPath}");

try
{
    using var connection = new SqliteConnection($"Data Source={dbPath}");
    connection.Open();

    using var transaction = connection.BeginTransaction();

    // Insert a row to verify the lock was actually held and committed
    // This row contributes to the final count check in the shell script
    using (var cmd = connection.CreateCommand())
    {
        cmd.Transaction = transaction;
        cmd.CommandText = "INSERT INTO sensitive_data (id, content) VALUES (999, 'Locked content');";
        cmd.ExecuteNonQuery();
    }

    // Create signal file to notify shell script that lock is held
    File.WriteAllText(signalFile, "LOCKED");

    Console.WriteLine($"🔒 [Locker] Lock acquired. Signal created. Sleeping for {durationMs}ms...");
    Thread.Sleep(durationMs);

    transaction.Commit();
    Console.WriteLine("🔓 [Locker] Lock released (committed).");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ [Locker] Error: {ex.Message}");
    Environment.Exit(1);
}
finally
{
    if (File.Exists(signalFile)) File.Delete(signalFile);
}
