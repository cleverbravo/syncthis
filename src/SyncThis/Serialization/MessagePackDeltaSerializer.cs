using System.Reflection;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Serialization;

public class MessagePackDeltaSerializer : IDelta
{
    private readonly MessagePackSerializerOptions _options;

    public MessagePackDeltaSerializer()
    {
        _options = MessagePack.Resolvers.ContractlessStandardResolver.Options;
    }

    public Result<byte[]> ComputeDelta(object current, object? previous)
    {
        try
        {
            if (previous is null)
                return TakeSnapshot(current);

            var currentProps = GetPublicProperties(current.GetType());
            var previousSnapshot = TakeSnapshot(previous);
            if (previousSnapshot.IsFailure)
                return Result<byte[]>.Failure(previousSnapshot.Error);

            var delta = new Dictionary<string, object?>();
            foreach (var prop in currentProps)
            {
                var curVal = prop.GetValue(current);
                var prevVal = prop.GetValue(previous);
                if (!Equals(curVal, prevVal))
                    delta[prop.Name] = curVal;
            }

            return Result<byte[]>.Success(MessagePack.MessagePackSerializer.Serialize(delta, _options));
        }
        catch (Exception ex)
        {
            return Result<byte[]>.Failure("DELTA_COMPUTE_FAILED", ex.Message);
        }
    }

    public Result<object> ApplyDelta(object target, byte[] delta, Type type)
    {
        try
        {
            var deltaDict = MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, object?>>(delta, _options);
            var props = GetPublicProperties(type);

            foreach (var kvp in deltaDict)
            {
                var prop = props.FirstOrDefault(p => p.Name == kvp.Key);
                if (prop is not null && kvp.Value is not null)
                {
                    var converted = ConvertValue(kvp.Value, prop.PropertyType);
                    prop.SetValue(target, converted);
                }
            }

            return Result<object>.Success(target);
        }
        catch (Exception ex)
        {
            return Result<object>.Failure("DELTA_APPLY_FAILED", ex.Message);
        }
    }

    public Result<byte[]> TakeSnapshot(object obj)
    {
        try
        {
            var snapshot = GetPublicProperties(obj.GetType())
                .ToDictionary(p => p.Name, p => p.GetValue(obj));
            return Result<byte[]>.Success(MessagePack.MessagePackSerializer.Serialize(snapshot, _options));
        }
        catch (Exception ex)
        {
            return Result<byte[]>.Failure("SNAPSHOT_FAILED", ex.Message);
        }
    }

    public Result<object> FromSnapshot(byte[] snapshot, Type type)
    {
        try
        {
            var obj = Activator.CreateInstance(type)!;
            var snapshotDict = MessagePack.MessagePackSerializer.Deserialize<Dictionary<string, object?>>(snapshot, _options);
            var props = GetPublicProperties(type);

            foreach (var kvp in snapshotDict)
            {
                var prop = props.FirstOrDefault(p => p.Name == kvp.Key);
                if (prop is not null && kvp.Value is not null)
                {
                    var converted = ConvertValue(kvp.Value, prop.PropertyType);
                    prop.SetValue(obj, converted);
                }
            }

            return Result<object>.Success(obj);
        }
        catch (Exception ex)
        {
            return Result<object>.Failure("SNAPSHOT_DESERIALIZE_FAILED", ex.Message);
        }
    }

    private static PropertyInfo[] GetPublicProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

    private static object? ConvertValue(object value, Type targetType)
    {
        if (value is null) return null;

        var valueType = value.GetType();
        if (valueType == targetType || targetType.IsAssignableFrom(valueType))
            return value;

        var underlying = Nullable.GetUnderlyingType(targetType);
        var nonNullable = underlying ?? targetType;

        if (nonNullable.IsEnum)
        {
            try { return Enum.Parse(nonNullable, value.ToString()!); }
            catch { return value; }
        }

        if (nonNullable == typeof(Guid) && value is string guidString)
        {
            try { return Guid.Parse(guidString); }
            catch { return value; }
        }

        if ((nonNullable == typeof(DateTime) || nonNullable == typeof(DateTimeOffset))
            && value is string dateString)
        {
            try { return DateTime.Parse(dateString, System.Globalization.CultureInfo.InvariantCulture); }
            catch { return value; }
        }

        if (nonNullable == typeof(TimeSpan) && value is string tsString)
        {
            try { return TimeSpan.Parse(tsString, System.Globalization.CultureInfo.InvariantCulture); }
            catch { return value; }
        }

        if (value is IConvertible convertible)
        {
            try { return convertible.ToType(nonNullable, null); }
            catch { return value; }
        }

        return value;
    }
}
