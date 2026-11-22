using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using Jint;
using Jint.Native;
using Jint.Native.Error;
using JsTypes = Jint.Runtime.Types;

namespace DtPipe.Transformers.Services;

internal static class JsValueConversion
{
    private static readonly ConditionalWeakTable<Engine, ErrorConstructor> RefErrorCache = new();

    public static ErrorConstructor GetReferenceErrorConstructor(Engine engine)
        => RefErrorCache.GetValue(engine, static e => (ErrorConstructor)e.GetValue("ReferenceError"));

    public static JsValue ClrToJsValue(Engine engine, object? val) => val switch
    {
        null or DBNull => JsValue.Null,
        double d => JsNumber.Create(d),
        int i => JsNumber.Create(i),
        long l => JsNumber.Create(l),
        float f => JsNumber.Create(f),
        decimal m => JsNumber.Create(m),
        short s => JsNumber.Create(s),
        ushort us => JsNumber.Create(us),
        sbyte sb => JsNumber.Create(sb),
        byte b => JsNumber.Create(b),
        uint ui => JsNumber.Create((double)ui),
        ulong ul => JsNumber.Create((double)ul),
        string str => JsString.Create(str),
        bool flag => flag ? JsBoolean.True : JsBoolean.False,
        _ => JsValue.FromObject(engine, val)
    };

    public static object? JsValueToClr(JsValue val)
    {
        if (val.IsUndefined() || val.IsNull())
            return null;

        return val.Type switch
        {
            JsTypes.Number => val.AsNumber(),
            JsTypes.String => val.AsString(),
            JsTypes.Boolean => val.AsBoolean(),
            _ => val.ToObject()
        };
    }

    public static string JsValueToString(JsValue val)
    {
        if (val.IsString())
            return val.AsString();

        if (val.IsNumber())
            return val.AsNumber().ToString(CultureInfo.InvariantCulture);

        return Convert.ToString(JsValueToClr(val), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    public static void AppendJsValueToBuilder(
        IArrowArrayBuilder builder,
        JsValue val,
        Action<object?> fallbackAppender)
    {
        if (val.IsUndefined() || val.IsNull())
        {
            AppendNullFast(builder);
            return;
        }

        switch (builder)
        {
            case DoubleArray.Builder b when val.IsNumber():
                b.Append(val.AsNumber());
                return;
            case FloatArray.Builder b when val.IsNumber():
                b.Append((float)val.AsNumber());
                return;
            case Int32Array.Builder b when val.IsNumber():
                b.Append(Convert.ToInt32(val.AsNumber()));
                return;
            case Int64Array.Builder b when val.IsNumber():
                b.Append(Convert.ToInt64(val.AsNumber()));
                return;
            case Int16Array.Builder b when val.IsNumber():
                b.Append(Convert.ToInt16(val.AsNumber()));
                return;
            case Int8Array.Builder b when val.IsNumber():
                b.Append(Convert.ToSByte(val.AsNumber()));
                return;
            case UInt32Array.Builder b when val.IsNumber():
                b.Append(Convert.ToUInt32(val.AsNumber()));
                return;
            case UInt64Array.Builder b when val.IsNumber():
                b.Append(Convert.ToUInt64(val.AsNumber()));
                return;
            case UInt16Array.Builder b when val.IsNumber():
                b.Append(Convert.ToUInt16(val.AsNumber()));
                return;
            case UInt8Array.Builder b when val.IsNumber():
                b.Append(Convert.ToByte(val.AsNumber()));
                return;
            case BooleanArray.Builder b when val.IsBoolean():
                b.Append(val.AsBoolean());
                return;
            case Decimal128Array.Builder b when val.IsNumber():
                b.Append(Convert.ToDecimal(val.AsNumber()));
                return;
            case StringArray.Builder b:
                b.Append(JsValueToString(val));
                return;
            default:
                fallbackAppender(JsValueToClr(val));
                return;
        }
    }

    public static void AppendNullFast(IArrowArrayBuilder builder)
    {
        switch (builder)
        {
            case DoubleArray.Builder b: b.AppendNull(); break;
            case FloatArray.Builder b: b.AppendNull(); break;
            case Int32Array.Builder b: b.AppendNull(); break;
            case Int64Array.Builder b: b.AppendNull(); break;
            case Int16Array.Builder b: b.AppendNull(); break;
            case Int8Array.Builder b: b.AppendNull(); break;
            case UInt32Array.Builder b: b.AppendNull(); break;
            case UInt64Array.Builder b: b.AppendNull(); break;
            case UInt16Array.Builder b: b.AppendNull(); break;
            case UInt8Array.Builder b: b.AppendNull(); break;
            case BooleanArray.Builder b: b.AppendNull(); break;
            case Decimal128Array.Builder b: b.AppendNull(); break;
            case StringArray.Builder b: b.AppendNull(); break;
            default: ArrowTypeMapper.AppendNull(builder); break;
        }
    }
}
