using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.Loader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Messaging.Streaming;

namespace Intropy.Framework.Hosting.Test.Loader;

public sealed record OrderCreated(string OrderId, string Customer);

public sealed record OrderCancelled(string OrderId, string Reason);

public sealed class JsonDeserializer<T> : DeserializeStep<T, Context>
{
    protected override Task<(BusinessStepResult<T> Result, Context Context)> DeserializeAsync(CloudEvent cloudEvent,
        Context context)
    {
        var value = JsonSerializer.Deserialize<T>(cloudEvent.Data?.ToString() ?? "")!;
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

/// <summary>Records what it sends; fails while <see cref="Failure"/> is set, and waits for
/// <see cref="Gate"/> when one is set.</summary>
public sealed class RecordingSender<T> : SendStep<T, Context>
{
    private readonly List<(T Value, bool IsRetry)> _sent = [];

    public IReadOnlyList<(T Value, bool IsRetry)> Sent
    {
        get { lock (_sent) return [.. _sent]; }
    }

    public Exception? Failure { get; set; }

    public TaskCompletionSource? Gate { get; set; }

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public override async Task<(TechnicalStepResult<T> Result, Context Context)> ExecuteAsync(T input,
        Context context, CancellationToken ct)
    {
        Entered.TrySetResult();
        if (Gate is not null)
            await Gate.Task.WaitAsync(ct);
        if (Failure is not null)
            throw Failure;
        lock (_sent) _sent.Add((input, context.IsRetry));
        return (new TechnicalStepResult<T>.Success(input), context);
    }
}

/// <summary>
/// Stands in for the sidecar's streaming subscriptions: records each subscription the loader opens,
/// delivers messages to its handler the way the sidecar would, and can break the stream.
/// </summary>
public sealed class FakeStreamingSubscriber : IStreamingSubscriber
{
    private readonly List<Subscription> _subscriptions = [];

    /// <summary>How many of the next subscribe calls fail, as if the sidecar were not up yet.</summary>
    public int FailNextSubscribes { get; set; }

    public int SubscribeAttempts { get; private set; }

    public IReadOnlyList<Subscription> Subscriptions
    {
        get { lock (_subscriptions) return [.. _subscriptions]; }
    }

    public Subscription Current => Subscriptions[^1];

    /// <summary>Waits until the loader has opened <paramref name="count"/> subscriptions in all.</summary>
    public async Task WaitForSubscriptionsAsync(int count)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (Subscriptions.Count < count)
            await Task.Delay(10, timeout.Token);
    }

    public Task<IDaprSubscription> SubscribeAsync(string pubSubName, string topicName, DaprSubscriptionOptions options,
        TopicMessageHandler handler, CancellationToken cancellationToken)
    {
        SubscribeAttempts++;
        if (FailNextSubscribes > 0)
        {
            FailNextSubscribes--;
            throw new InvalidOperationException("The sidecar is not available");
        }

        var subscription = new Subscription(pubSubName, topicName, options, handler);
        lock (_subscriptions) _subscriptions.Add(subscription);
        return Task.FromResult<IDaprSubscription>(subscription);
    }

    /// <summary>Delivers a CloudEvent on the current subscription and returns its ack.</summary>
    public Task<TopicResponseAction> DeliverAsync(string eventType, string subject, string data,
        bool redelivery = false) => Current.DeliverAsync(eventType, subject, data, redelivery);

    public sealed class Subscription(string pubSubName, string topicName, DaprSubscriptionOptions options,
        TopicMessageHandler handler) : IDaprSubscription
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string PubSubName => pubSubName;
        public string TopicName => topicName;
        public DaprSubscriptionOptions Options => options;
        public bool Disposed { get; private set; }
        public Task Completion => _completion.Task;

        public Task<TopicResponseAction> DeliverAsync(string eventType, string subject, string data, bool redelivery)
        {
            var extensions = new Dictionary<string, Value>
            {
                ["subject"] = Value.ForString(subject),
                ["time"] = Value.ForString(DateTimeOffset.UtcNow.ToString("O"))
            };
            if (redelivery)
                extensions["retrycount"] = Value.ForString("1");

            var message = new TopicMessage(Guid.NewGuid().ToString(), "urn:test:source", eventType, "1.0",
                "application/json", topicName, pubSubName)
            {
                Data = Encoding.UTF8.GetBytes(data),
                Extensions = extensions
            };
            return handler(message, CancellationToken.None);
        }

        /// <summary>Breaks the stream, as a lost connection to the sidecar would.</summary>
        public void Break() => _completion.TrySetException(new InvalidOperationException("The stream broke"));

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            _completion.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }
}
