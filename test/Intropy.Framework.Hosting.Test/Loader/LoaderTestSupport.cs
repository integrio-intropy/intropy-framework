using System.Text.Json;
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

    /// <summary>The bytes the pipeline was last handed, recorded before anything interprets them —
    /// the only place a test can see what a real sidecar actually delivered.</summary>
    public ReadOnlyMemory<byte> LastInput { get; private set; }

    /// <summary>Clears <see cref="LastInput"/>, so a test can wait for the next delivery rather
    /// than race a previous one.</summary>
    public void Reset() => LastInput = default;

    public override Task<(BusinessStepResult<T> Result, Context Context)> ExecuteAsync(ReadOnlyMemory<byte> input,
        Context context, CancellationToken ct)
    {
        LastInput = input.ToArray();

        // A payload that will not parse is a verdict on the data, not on the circumstances: the same
        // bytes fail identically on every attempt. Saying so explicitly (rather than throwing and
        // taking the retriable default) keeps such a message acknowledged once its incident is
        // routed, so tests that publish unparseable payloads leave no redelivery traffic behind for
        // later tests in the same fixture to trip over.
        T? value;
        try
        {
            value = JsonSerializer.Deserialize<T>(input.Span, s_options);
        }
        catch (JsonException e)
        {
            var incident = new BusinessIncidentData
            {
                Description = "The payload is not valid JSON for this pipeline",
                Context = new Dictionary<string, string> { ["error"] = e.Message }
            };
            return Task.FromResult<(BusinessStepResult<T>, Context)>(
                (new BusinessStepResult<T>.Failure(incident, Retriable: false), context));
        }

        return Task.FromResult<(BusinessStepResult<T>, Context)>((new BusinessStepResult<T>.Success(value!), context));
    }
}

/// <summary>Accepts every input, or rejects the ones <paramref name="reject"/> matches as a
/// business failure — terminal by default, as a validator's verdict on the data is, or retriable
/// when <paramref name="retriable"/> is set.</summary>
public sealed class TestValidator<T>(Func<T, bool>? reject = null, bool retriable = false)
    : ValidateStep<T, Context>
{
    public override Task<(BusinessStepResult<T> Result, Context Context)> ExecuteAsync(T input, Context context,
        CancellationToken ct)
    {
        BusinessStepResult<T> result = reject?.Invoke(input) == true
            ? new BusinessStepResult<T>.Failure(new BusinessIncidentData
            {
                Description = "Rejected by the test validator", Context = new Dictionary<string, string>()
            }, retriable)
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

    /// <summary>The context of the last message that reached the sender, for asserting on what the
    /// host put there before the pipeline ran.</summary>
    public Context? LastContext { get; private set; }

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
        LastContext = context;
        lock (_sent) _sent.Add((input, context.IsRetry));
        return (new TechnicalStepResult<T>.Success(input), context);
    }
}
