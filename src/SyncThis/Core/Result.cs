namespace SyncThis.Core;

public readonly struct Error : IEquatable<Error>
{
    public string Code { get; }
    public string Message { get; }

    public Error(string code, string message)
    {
        Code = code;
        Message = message;
    }

    public static Error None => default;

    public bool Equals(Error other) => Code == other.Code && Message == other.Message;
    public override bool Equals(object? obj) => obj is Error other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Code, Message);
    public static bool operator ==(Error left, Error right) => left.Equals(right);
    public static bool operator !=(Error left, Error right) => !left.Equals(right);
}

public readonly struct Result<T>
{
    private readonly T? _value;
    private readonly Error _error;

    public bool IsSuccess { get; }
    public bool IsFailure => !IsSuccess;

    public T Value => IsSuccess ? _value! : throw new InvalidOperationException("Cannot access Value of a failed result.");
    public Error Error => IsFailure ? _error : Error.None;

    private Result(T value)
    {
        IsSuccess = true;
        _value = value;
        _error = Error.None;
    }

    private Result(Error error)
    {
        IsSuccess = false;
        _value = default;
        _error = error;
    }

    public static Result<T> Success(T value) => new(value);
    public static Result<T> Failure(Error error) => new(error);
    public static Result<T> Failure(string code, string message) => new(new Error(code, message));

    public TOut Match<TOut>(Func<T, TOut> onSuccess, Func<Error, TOut> onFailure)
        => IsSuccess ? onSuccess(_value!) : onFailure(_error);

    public void Switch(Action<T> onSuccess, Action<Error> onFailure)
    {
        if (IsSuccess) onSuccess(_value!);
        else onFailure(_error);
    }
}

public readonly struct Result
{
    private readonly Error _error;

    public bool IsSuccess { get; }
    public bool IsFailure => !IsSuccess;
    public Error Error => IsFailure ? _error : Error.None;

    public Result()
    {
        IsSuccess = true;
        _error = Error.None;
    }

    private Result(Error error)
    {
        IsSuccess = false;
        _error = error;
    }

    public static Result Success() => new();
    public static Result Failure(Error error) => new(error);
    public static Result Failure(string code, string message) => new(new Error(code, message));

    public TOut Match<TOut>(Func<TOut> onSuccess, Func<Error, TOut> onFailure)
        => IsSuccess ? onSuccess() : onFailure(_error);

    public void Switch(Action onSuccess, Action<Error> onFailure)
    {
        if (IsSuccess) onSuccess();
        else onFailure(_error);
    }
}
