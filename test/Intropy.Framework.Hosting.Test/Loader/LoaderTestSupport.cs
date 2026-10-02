using System.Text.Json;
using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

namespace Intropy.Framework.Hosting.Test.Loader;

public sealed record OrderCreated(string OrderId, string Customer);

public sealed record OrderCancelled(string OrderId, string Reason);

public sealed class JsonDeserializer<T> : DeserializeStep<T, Context>
{
    // The framework publishes camelCase payloads.
    private static readonly JsonSerializerOptions s_options = new(JsonSerializerDefaults.Web);

    protected override Task<(BusinessStepResult<T> Result, Context Context)> DeserializeAsync(CloudEvent cloudEvent,
        Context context)
    {
        var value = JsonSerializer.Deserialize<T>(cloudEvent.Data?.ToString() ?? "", s_options)!;
        return Task.FromResult<(BusinessStepResult<T>, Context)>((new BusinessStepResult<T>.Success(value), context));
    }
}

/// <summary>Accepts every input, or rejects the ones <paramref name="reject"/> matches as a
/// business failure.</summary>
public sealed class TestValidator<T>(Func<T, bool>? reject = null) : ValidateStep<T, Context>
{
    public override Task<(BusinessStepResult<T> Result, Context Context)> ExecuteAsync(T input, Context context,
        CancellationToken ct)
    {
        BusinessStepResult<T> result = reject?.Invoke(input) == true
            ? new BusinessStepResult<T>.Failure(new BusinessIncidentData
            {
                Description = "Rejected by the test validator", Context = new Dictionary<string, string>()
            })
            : new BusinessStepResult<T>.Success(input);
        return Task.FromResult<(BusinessStepResult<T>, Context)>((result, context));
    }
}

public sealed class IdentityTransformer<T> : TransformStep<T, T, Context>
{
    public override Task<(TechnicalStepResult<T> Result, Context Context)> ExecuteAsync(T input, Context context,
        CancellationToken ct) =>
        Task.FromResult<(TechnicalStepResult<T>, Context)>((new TechnicalStepResult<T>.Success(input), context));
}

/// <summary>Records what it sends and how often it was called; fails while <see cref="Failure"/> is
/// set or for the next <see cref="FailNext"/> calls, and waits for <see cref="Gate"/> when one is
/// set.</summary>
public sealed class RecordingSender<T> : SendStep<T, Context>
{
    private readonly List<(T Value, bool IsRetry)> _sent = [];
    private int _attempts;
    private int _failNext;

    public IReadOnlyList<(T Value, bool IsRetry)> Sent
    {
        get { lock (_sent) return [.. _sent]; }
    }

    public Exception? Failure { get; set; }

    /// <summary>How many times the sender was called, failures included.</summary>
    public int Attempts => Volatile.Read(ref _attempts);

    /// <summary>Fails the next <paramref name="count"/> calls, then sends normally.</summary>
    public void FailNext(int count) => Volatile.Write(ref _failNext, count);

    public TaskCompletionSource? Gate { get; set; }

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override async Task<(TechnicalStepResult<T> Result, Context Context)> ExecuteAsync(T input,
        Context context, CancellationToken ct)
    {
        Interlocked.Increment(ref _attempts);
        Entered.TrySetResult();
        if (Gate is not null)
            await Gate.Task.WaitAsync(ct);
        if (Failure is not null)
            throw Failure;
        if (Interlocked.Decrement(ref _failNext) >= 0)
            throw new InvalidOperationException("Destination unreachable (scripted failure)");
        lock (_sent) _sent.Add((input, context.IsRetry));
        return (new TechnicalStepResult<T>.Success(input), context);
    }
}
