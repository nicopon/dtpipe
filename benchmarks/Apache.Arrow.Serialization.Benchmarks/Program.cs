using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Serialization;

namespace Apache.Arrow.Serialization.Benchmarks;

public class Program
{
    public static async Task Main(string[] args)
    {
        const int N = 100000;
        const int Iterations = 5;

        Console.WriteLine($"--- Benchmark Memory & Speed with {N} IoTTelemetry items ---");

        var data = Enumerable.Range(0, N).Select(i => new IoTTelemetry
        {
            DeviceId = Guid.NewGuid(),
            DeviceName = "WeatherStation-" + i,
            Timestamp = DateTime.UtcNow,
            Readings = new[] { 22.5, 60.0, 1013.2 },
            Metadata = new Dictionary<string, string> { { "Location", "Paris" }, { "Model", "TX-100" } },
            SensorInfo = new SensorMetadata { SerialNumber = "SN" + i, FirmwareVersion = "v1.2", CalibrationYear = 2025 }
        }).ToList();

        // Warmup
        await ArrowSerializer.SerializeAsync(data);
        var batch = await ArrowSerializer.SerializeAsync(data);
        var jsonData = JsonSerializer.SerializeToUtf8Bytes(data);
        ArrowDeserializer.Deserialize<IoTTelemetry>(batch).ToList();
        JsonSerializer.Deserialize<List<IoTTelemetry>>(jsonData);

        Console.WriteLine("Warmup complete. Starting benchmark...");

        // Arrow Serialize
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var allocBefore = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            using var b = await ArrowSerializer.SerializeAsync(data);
        }
        sw.Stop();
        var allocAfter = GC.GetAllocatedBytesForCurrentThread();
        var arrowSerTime = sw.ElapsedMilliseconds / (double)Iterations;
        var arrowSerAlloc = (allocAfter - allocBefore) / (double)Iterations;
        Console.WriteLine($"Arrow Serialize:   {arrowSerTime,8:F2} ms | {arrowSerAlloc / 1024.0 / 1024.0,8:F2} MB allocated");

        // Json Serialize
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        allocBefore = GC.GetAllocatedBytesForCurrentThread();
        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            JsonSerializer.SerializeToUtf8Bytes(data);
        }
        sw.Stop();
        allocAfter = GC.GetAllocatedBytesForCurrentThread();
        var jsonSerTime = sw.ElapsedMilliseconds / (double)Iterations;
        var jsonSerAlloc = (allocAfter - allocBefore) / (double)Iterations;
        Console.WriteLine($"JSON Serialize:    {jsonSerTime,8:F2} ms | {jsonSerAlloc / 1024.0 / 1024.0,8:F2} MB allocated");

        // Arrow Deserialize
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        allocBefore = GC.GetAllocatedBytesForCurrentThread();
        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            var result = ArrowDeserializer.Deserialize<IoTTelemetry>(batch).ToList();
        }
        sw.Stop();
        allocAfter = GC.GetAllocatedBytesForCurrentThread();
        var arrowDeserTime = sw.ElapsedMilliseconds / (double)Iterations;
        var arrowDeserAlloc = (allocAfter - allocBefore) / (double)Iterations;
        Console.WriteLine($"Arrow Deserialize: {arrowDeserTime,8:F2} ms | {arrowDeserAlloc / 1024.0 / 1024.0,8:F2} MB allocated");

        // Json Deserialize
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        allocBefore = GC.GetAllocatedBytesForCurrentThread();
        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            JsonSerializer.Deserialize<List<IoTTelemetry>>(jsonData);
        }
        sw.Stop();
        allocAfter = GC.GetAllocatedBytesForCurrentThread();
        var jsonDeserTime = sw.ElapsedMilliseconds / (double)Iterations;
        var jsonDeserAlloc = (allocAfter - allocBefore) / (double)Iterations;
        Console.WriteLine($"JSON Deserialize:  {jsonDeserTime,8:F2} ms | {jsonDeserAlloc / 1024.0 / 1024.0,8:F2} MB allocated");

        Console.WriteLine("\n--- Ratio Analysis (Arrow/JSON) ---");
        Console.WriteLine($"Serialization Speed:   {jsonSerTime / arrowSerTime:F2}x faster");
        Console.WriteLine($"Serialization Alloc:   {arrowSerAlloc / jsonSerAlloc:F2}x allocations");
        Console.WriteLine($"Deserialization Speed: {jsonDeserTime / arrowDeserTime:F2}x faster");
        Console.WriteLine($"Deserialization Alloc: {arrowDeserAlloc / jsonDeserAlloc:F2}x allocations");

        // --- Dynamic Benchmarks ---
        Console.WriteLine($"\n--- Benchmark Dynamic with {N} items ---");
        
        var dynamicData = Enumerable.Range(0, N).Select(i => {
            dynamic obj = new System.Dynamic.ExpandoObject();
            obj.Id = i;
            obj.Name = "Item-" + i;
            obj.Value = i * 1.5;
            return (object)obj;
        }).ToList();

        // Warmup Dynamic
        await ArrowSerializer.SerializeAsync(dynamicData);

        GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
        allocBefore = GC.GetAllocatedBytesForCurrentThread();
        sw.Restart();
        for (int i = 0; i < Iterations; i++)
        {
            using var b = await ArrowSerializer.SerializeAsync(dynamicData);
        }
        sw.Stop();
        allocAfter = GC.GetAllocatedBytesForCurrentThread();
        var dynamicTime = sw.ElapsedMilliseconds / (double)Iterations;
        var dynamicAlloc = (allocAfter - allocBefore) / (double)Iterations;
        Console.WriteLine($"Arrow Dynamic (Expando): {dynamicTime,8:F2} ms | {dynamicAlloc / 1024.0 / 1024.0,8:F2} MB allocated");
        Console.WriteLine($"Speed vs Static:        {dynamicTime / arrowSerTime:F2}x slower");
    }
}
