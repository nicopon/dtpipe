using System;
using System.Collections.Generic;

namespace Apache.Arrow.Serialization.Benchmarks;

public enum LogLevel { Debug, Info, Warning, Error, Critical }
public enum OrderStatus { Pending, Processing, Shipped, Delivered, Cancelled }
public enum Currency { USD, EUR, GBP, JPY, CAD }

public class SensorMetadata
{
    public string SerialNumber { get; set; } = string.Empty;
    public string FirmwareVersion { get; set; } = string.Empty;
    public int CalibrationYear { get; set; }
}

public class IoTTelemetry
{
    public Guid DeviceId { get; set; }
    public string DeviceName { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public double[] Readings { get; set; } = System.Array.Empty<double>();
    public Dictionary<string, string> Metadata { get; set; } = new();
    public SensorMetadata SensorInfo { get; set; } = new();
}

public class OrderItem
{
    public string ProductSku { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class ECommerceOrder
{
    public long OrderId { get; set; }
    public OrderStatus Status { get; set; }
    public List<OrderItem> Items { get; set; } = new();
    public decimal TotalAmount { get; set; }
    public Currency Currency { get; set; }
    public DateTimeOffset OrderedAt { get; set; }
}
