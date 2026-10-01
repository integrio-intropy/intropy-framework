using System.Diagnostics;
using Dapr.Messaging.PublishSubscribe;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Messaging.Streaming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Loader;

/// <summary>
/// A loader's process lifetime: consumes its topic through a Dapr streaming subscription from host
/// start until host stop. A broken stream is reopened after <see cref="LoaderOptions.ReconnectDelay"/>.
/// The loader stops the host with exit code 1 when a route's pipeline cannot be composed or the
/// subscription cannot be opened within <see cref="LoaderOptions.SubscribeTimeout"/>. On stop, the
/// message in flight gets <see cref="LoaderOptions.ShutdownGracePeriod"/> to finish before it is
/// interrupted and left for redelivery.
/// </summary>
internal sealed class LoaderService(
    IStreamingSubscriber subscriber,
    LoaderMessageHandler handler,
    LoaderRouteTable routes,
    LoaderOptions options,
    string componentName,
    IServiceScopeFactory scopes,
    IHostApplicationLifetime lifetime,
    ILogger<LoaderService> logger) : BackgroundService
{
    /// <summary>Subscription teardown happens while the sidecar may be going away; bound it.</summary>
    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(5);

    private readonly InFlightMessages _inFlight = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        try
        {
            routes.Verify(scopes, componentName);
            logger.LogInformation(
                "Loader {Component} consuming topic {Topic} on {PubSub}; routes: {Routes}; unrouted messages: {Unrouted}",
                componentName, options.TopicName, options.PubSubName, string.Join(", ", routes.Routes.Select(r => r.Name)),
                routes.IsRouting ? options.Unrouted.ToString() : "n/a");

            while (!stoppingToken.IsCancellationRequested)
                await ConsumeUntilStoppedOrBrokenAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Stopped while (re)subscribing.
        }
        catch (Exception e)
        {
            logger.LogCritical(e, "Loader {Component} stopped consuming topic {Topic}; stopping the host",
                componentName, options.TopicName);
            Environment.ExitCode = 1;
            lifetime.StopApplication();
        }
    }

    public override void Dispose()
    {
        _inFlight.Dispose();
        base.Dispose();
    }

    private async Task ConsumeUntilStoppedOrBrokenAsync(CancellationToken stoppingToken)
    {
        var fault = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriptionOptions = new DaprSubscriptionOptions(
            new MessageHandlingPolicy(options.MaxMessageProcessingTime, TopicResponseAction.Retry))
        {
            DeadLetterTopic = options.DeadLetterTopic,
            ErrorHandler = e =>
            {
                fault.TrySetResult(e);
                return Task.CompletedTask;
            }
        };

        var subscription = await SubscribeAsync(subscriptionOptions, stoppingToken);
        try
        {
            var stopped = Task.Delay(Timeout.Infinite, stoppingToken);
            var ended = await Task.WhenAny(stopped, subscription.Completion, fault.Task);
            if (ended == stopped)
                return;

            var error = fault.Task.IsCompleted ? await fault.Task : subscription.Completion.Exception?.GetBaseException();
            logger.LogWarning(error, "The subscription to topic {Topic} broke; reopening it in {Delay}",
                options.TopicName, options.ReconnectDelay);
        }
        finally
        {
            // Drain while the stream is still open, so the acks of messages in flight reach the sidecar.
            if (stoppingToken.IsCancellationRequested)
                await _inFlight.StopAsync(options.ShutdownGracePeriod, logger);

            await CloseAsync(subscription);
        }

        await Task.Delay(options.ReconnectDelay, stoppingToken);
    }

    private async Task<IDaprSubscription> SubscribeAsync(DaprSubscriptionOptions subscriptionOptions,
        CancellationToken stoppingToken)
    {
        // The sidecar may start after the app: keep trying for a bounded time rather than crash-loop.
        var elapsed = Stopwatch.StartNew();
        for (var attempt = 1;; attempt++)
        {
            try
            {
                return await subscriber.SubscribeAsync(options.PubSubName, options.TopicName, subscriptionOptions,
                    HandleAsync, stoppingToken);
            }
            catch (Exception e) when (e is not OperationCanceledException && elapsed.Elapsed < options.SubscribeTimeout)
            {
                logger.LogWarning("Subscribing to topic {Topic} failed (attempt {Attempt}): {Error}; retrying",
                    options.TopicName, attempt, e.Message);
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(attempt, 5)), stoppingToken);
            }
        }
    }

    private async Task<TopicResponseAction> HandleAsync(TopicMessage message, CancellationToken messageCancellation)
    {
        // Delivered after stop began: leave it for the next consumer.
        if (!_inFlight.TryEnter())
            return TopicResponseAction.Retry;

        try
        {
            using var cancellation =
                CancellationTokenSource.CreateLinkedTokenSource(_inFlight.Interrupt, messageCancellation);
            var outcome = await handler.ProcessAsync(TopicMessageReader.Read(message), message.Extensions,
                options.TopicName, _inFlight.Interrupt, cancellation.Token);
            return LoaderAcks.ToResponse(outcome.Outcome, options.Unrouted);
        }
        finally
        {
            _inFlight.Exit();
        }
    }

    private async Task CloseAsync(IDaprSubscription subscription)
    {
        try
        {
            await subscription.DisposeAsync().AsTask().WaitAsync(s_closeTimeout, CancellationToken.None);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Closing the subscription to topic {Topic} did not complete cleanly",
                options.TopicName);
        }
    }
}
