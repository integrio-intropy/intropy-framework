using CloudNative.CloudEvents;
using Intropy.Contracts.BusinessIncidentService;
using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.BulkLoader.Steps;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Testing.Delivery;
using Intropy.Framework.Testing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A generic host running a bulk loader against fakes: the sidecar's bulk pushes to the loader's app
/// callback (<see cref="Callback"/>), the business-incident client, and recording batch senders.
/// The bulk counterpart of <see cref="LoaderHost"/>.
/// </summary>
public sealed class BulkLoaderHost : IAsyncDisposable
{
    public IHost Host { get; private set; } = null!;
    public AppCallbackDelivery Callback { get; private set; } = null!;
    public FakeBusinessIncidentServiceClient Incidents { get; } = new();
    public RecordingBatchSender<OrderCreated> CreatedSender { get; } = new();
    public RecordingBatchSender<OrderCancelled> CancelledSender { get; } = new();

    /// <summary>A bulk loader routing <see cref="LoaderHost.Created"/> and
    /// <see cref="LoaderHost.Cancelled"/>.</summary>
    /// <param name="reject">Created orders the validator fails as a terminal business incident.</param>
    /// <param name="configure">Further options, applied over the fixture's defaults.</param>
    /// <param name="extraServices">Registrations the loader resolves at run time.</param>
    /// <param name="createdLookup">A batch extractor for created orders, run before validation.</param>
    public static async Task<BulkLoaderHost> StartRoutingAsync(Func<OrderCreated, bool>? reject = null,
        Action<BulkLoaderOptions>? configure = null, Action<IServiceCollection>? extraServices = null,
        BatchExtractStep<OrderCreated, Context>? createdLookup = null)
    {
        var fixture = new BulkLoaderHost();
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddLogging();
        builder.Services.AddIntropyFramework(o =>
        {
            o.ComponentName = "test-bulk-loader";
            o.ServiceNamespace = "test";
        });
        builder.Services.AddSingleton<IBusinessIncidentServiceClient>(fixture.Incidents);
        extraServices?.Invoke(builder.Services);
        builder.Services.AddBulkLoader(
            options =>
            {
                options.PubSubName = LoaderHost.PubSub;
                options.TopicName = LoaderHost.Topic;
                options.CallbackPort = AppCallbackDelivery.AvailablePort();
                options.ShutdownGracePeriod = TimeSpan.FromSeconds(5);
                configure?.Invoke(options);
            },
            routes => routes
                .On<OrderCreated, OrderCreated, Context>(LoaderHost.Created,
                    (b, _) => Pipeline(b, fixture.CreatedSender, reject, createdLookup),
                    (metadata, isRetry) => new Context(metadata, isRetry))
                .On<OrderCancelled, OrderCancelled, Context>(LoaderHost.Cancelled,
                    (b, _) => Pipeline(b, fixture.CancelledSender),
                    (metadata, isRetry) => new Context(metadata, isRetry)));

        fixture.Host = builder.Build();
        await fixture.Host.StartAsync();

        var options = fixture.Host.Services.GetRequiredService<BulkLoaderOptions>();
        fixture.Callback = new AppCallbackDelivery(options.CallbackPort!.Value, options.PubSubName,
            options.TopicName);
        return fixture;
    }

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
        Callback.Dispose();
    }

    public Task<IReadOnlyList<DeliveryAck>> DeliverBulkAsync(params CloudEvent[] cloudEvents) =>
        Callback.DeliverBulkAsync(cloudEvents);

    private static BulkLoaderBuilder<T, T, Context> Pipeline<T>(BulkLoaderBuilder<T, T, Context> builder,
        RecordingBatchSender<T> sender, Func<T, bool>? reject = null, BatchExtractStep<T, Context>? lookup = null)
    {
        builder.WithDeserializer(new JsonDeserializer<T>());
        if (lookup is not null)
            builder.WithBatchExtractor(lookup);
        return builder
            .WithValidator(new TestValidator<T>(reject))
            .WithTransformer(new IdentityTransformer<T>())
            .WithBatchSender(sender)
            .WithBusinessIncidents(ctx => ctx.Metadata.GetValueOrDefault("cloudevent.subject", "unknown"),
                ctx => ctx.Metadata.GetValueOrDefault("cloudevent.subject", "unknown"));
    }
}

/// <summary>A batch sender that records every batch it is handed and accepts every entry, or fails
/// the whole call for the next <see cref="FailNext"/> calls.</summary>
public sealed class RecordingBatchSender<T> : BatchSendStep<T, Context>
{
    private readonly List<IReadOnlyList<T>> _batches = [];
    private int _attempts;
    private int _failNext;

    public int Attempts => Volatile.Read(ref _attempts);

    /// <summary>Fails the next <paramref name="count"/> calls, then sends normally.</summary>
    public void FailNext(int count) => Volatile.Write(ref _failNext, count);

    /// <summary>Each call's values, in the order the calls were made.</summary>
    public IReadOnlyList<IReadOnlyList<T>> Batches
    {
        get
        {
            lock (_batches)
                return [.. _batches];
        }
    }

    public override Task<IReadOnlyList<StepResult<T>>> ExecuteBatchAsync(IReadOnlyList<(T Value, Context Context)> entries,
        CancellationToken ct)
    {
        Interlocked.Increment(ref _attempts);
        if (Interlocked.Decrement(ref _failNext) >= 0)
            throw new InvalidOperationException("Destination unreachable (scripted failure)");
        lock (_batches)
            _batches.Add([.. entries.Select(e => e.Value)]);
        return Task.FromResult<IReadOnlyList<StepResult<T>>>(
            [.. entries.Select(e => (StepResult<T>)new StepResult<T>.Success(e.Value))]);
    }
}
