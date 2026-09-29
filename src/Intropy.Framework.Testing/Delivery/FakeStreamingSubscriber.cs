using CloudNative.CloudEvents;
using Dapr.Messaging.PublishSubscribe;
using Google.Protobuf.WellKnownTypes;
using Intropy.Framework.Hosting.Messaging.Streaming;

namespace Intropy.Framework.Testing.Delivery;

/// <summary>
/// Stands in for the Dapr sidecar's streaming subscriptions in a loader's integration tests: it
/// records every subscription the loader opens and delivers <see cref="CloudEvent"/>s to it the way
/// the sidecar does, returning the loader's ack.
/// </summary>
/// <remarks>
/// Register it before the host is built — <c>services.AddSingleton&lt;IStreamingSubscriber&gt;(fake)</c>
/// — to replace the Dapr client the loader would otherwise subscribe through. Wait for the loader to
/// subscribe (<see cref="WaitForSubscriptionAsync"/>) before delivering: it subscribes in the
/// background once the host has started.
/// </remarks>
public sealed class FakeStreamingSubscriber : IStreamingSubscriber
{
    private readonly List<FakeStreamingSubscription> _subscriptions = [];
    private int _failNextSubscribes;
    private int _subscribeAttempts;

    /// <summary>Every subscription the loader has opened, oldest first.</summary>
    public IReadOnlyList<FakeStreamingSubscription> Subscriptions
    {
        get
        {
            lock (_subscriptions) return [.. _subscriptions];
        }
    }

    /// <summary>The subscription the loader opened last.</summary>
    /// <exception cref="InvalidOperationException">The loader has not subscribed yet.</exception>
    public FakeStreamingSubscription Current => Subscriptions is { Count: > 0 } subscriptions
        ? subscriptions[^1]
        : throw new InvalidOperationException("The loader has not subscribed yet; await WaitForSubscriptionAsync first.");

    /// <summary>How many times the loader tried to subscribe, failed attempts included.</summary>
    public int SubscribeAttempts
    {
        get
        {
            lock (_subscriptions) return _subscribeAttempts;
        }
    }

    /// <summary>Makes the next <paramref name="count"/> subscribe calls fail, as when the sidecar is
    /// not up yet.</summary>
    public void FailNextSubscribes(int count)
    {
        lock (_subscriptions) _failNextSubscribes = count;
    }

    /// <summary>Waits until the loader has opened <paramref name="count"/> subscriptions in all.</summary>
    /// <exception cref="TimeoutException">It did not within <paramref name="timeout"/> (default 10 seconds).</exception>
    public async Task WaitForSubscriptionAsync(int count = 1, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (Subscriptions.Count < count)
        {
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"The loader opened {Subscriptions.Count} of {count} subscription(s).");
            await Task.Delay(10);
        }
    }

    /// <summary>Delivers <paramref name="cloudEvent"/> on the current subscription and returns the
    /// loader's ack.</summary>
    /// <param name="cloudEvent">The event, as its publisher sent it.</param>
    /// <param name="redelivery">Marks the delivery as a redelivery (the sidecar's <c>retrycount</c>
    /// extension), which the loader passes to its pipeline as a retry.</param>
    /// <param name="ct">Cancels the delivery, as the sidecar's per-message timeout would.</param>
    public Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, bool redelivery = false,
        CancellationToken ct = default) => Current.DeliverAsync(cloudEvent, redelivery, ct);

    /// <inheritdoc />
    public Task<IDaprSubscription> SubscribeAsync(string pubSubName, string topicName, DaprSubscriptionOptions options,
        TopicMessageHandler handler, CancellationToken cancellationToken)
    {
        lock (_subscriptions)
        {
            _subscribeAttempts++;
            if (_failNextSubscribes > 0)
            {
                _failNextSubscribes--;
                throw new InvalidOperationException("The Dapr sidecar is not available.");
            }

            var subscription = new FakeStreamingSubscription(pubSubName, topicName, options, handler);
            _subscriptions.Add(subscription);
            return Task.FromResult<IDaprSubscription>(subscription);
        }
    }
}

/// <summary>One streaming subscription a loader opened on a <see cref="FakeStreamingSubscriber"/>.</summary>
public sealed class FakeStreamingSubscription : IDaprSubscription
{
    private readonly TopicMessageHandler _handler;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal FakeStreamingSubscription(string pubSubName, string topicName, DaprSubscriptionOptions options,
        TopicMessageHandler handler)
    {
        PubSubName = pubSubName;
        TopicName = topicName;
        Options = options;
        _handler = handler;
    }

    /// <summary>The pub/sub component the loader subscribed through.</summary>
    public string PubSubName { get; }

    /// <summary>The topic the loader subscribed to.</summary>
    public string TopicName { get; }

    /// <summary>The options the loader subscribed with (message handling policy, dead-letter topic).</summary>
    public DaprSubscriptionOptions Options { get; }

    /// <summary>Whether the loader closed the subscription.</summary>
    public bool Disposed { get; private set; }

    /// <inheritdoc />
    public Task Completion => _completion.Task;

    /// <summary>Delivers <paramref name="cloudEvent"/> and returns the loader's ack.</summary>
    /// <param name="cloudEvent">The event, as its publisher sent it.</param>
    /// <param name="redelivery">Marks the delivery as a redelivery.</param>
    /// <param name="ct">Cancels the delivery.</param>
    public async Task<DeliveryAck> DeliverAsync(CloudEvent cloudEvent, bool redelivery = false,
        CancellationToken ct = default)
    {
        var (data, attributes) = SidecarEncoding.Encode(cloudEvent);
        var extensions = attributes.ToDictionary(a => a.Key, a => Value.ForString(a.Value), StringComparer.Ordinal);
        if (redelivery)
            extensions["retrycount"] = Value.ForString("1");

        var message = new TopicMessage(cloudEvent.Id ?? Guid.NewGuid().ToString(), cloudEvent.Source?.ToString() ?? "",
            cloudEvent.Type ?? "", "1.0", cloudEvent.DataContentType ?? "application/json", TopicName, PubSubName)
        {
            Data = data,
            Extensions = extensions
        };

        return await _handler(message, ct) switch
        {
            TopicResponseAction.Success => DeliveryAck.Success,
            TopicResponseAction.Drop => DeliveryAck.Drop,
            _ => DeliveryAck.Retry
        };
    }

    /// <summary>Breaks the stream, as a lost connection to the sidecar would: the loader reopens it.</summary>
    public void Break() => _completion.TrySetException(new InvalidOperationException("The subscription stream broke."));

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Disposed = true;
        _completion.TrySetResult();
        return ValueTask.CompletedTask;
    }
}
