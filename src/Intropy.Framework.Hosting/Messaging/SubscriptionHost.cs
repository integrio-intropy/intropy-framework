using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The subscription every message-consuming block hosts: a <see cref="MessageConsumer"/> and the
/// <see cref="MessageCallbackServer"/> the sidecar pushes its messages to, with the lifecycle
/// ordering that keeps deliveries safe — the callback starts before the block announces itself,
/// and on stop the consumer drains first (grace period, then interruption) and the callback stops
/// only after, so the acks of messages in flight still reach the sidecar. A block supplies its
/// <see cref="MessageHandler"/> and its completion condition (run until the host stops, or until
/// the queue goes idle); the start/stop protocol is the same for all of them.
/// </summary>
internal sealed class SubscriptionHost : IAsyncDisposable
{
    private readonly MessageCallbackServer _server;

    private SubscriptionHost(MessageConsumer consumer, MessageCallbackServer server, int port)
    {
        Consumer = consumer;
        _server = server;
        Port = port;
    }

    /// <summary>The consumer, for the block's completion condition (its in-flight messages and
    /// idle clock) and stop.</summary>
    internal MessageConsumer Consumer { get; }

    /// <summary>The port the callback listens on.</summary>
    internal int Port { get; }

    /// <summary>Starts the callback and returns the host once it listens.</summary>
    /// <param name="settings">What the consumer consumes and how long it gives each message.</param>
    /// <param name="handler">The block's message handler.</param>
    /// <param name="port">The callback port: the configured one, else <c>APP_PORT</c>, else 8080.</param>
    /// <param name="componentName">The component, for logs, spans and metrics.</param>
    /// <param name="loggerFactory">Creates the consumer's and the callback's loggers.</param>
    /// <param name="run">The span of the run consuming the messages, linked from each message's
    /// span; <see langword="default"/> for a long-running consumer.</param>
    /// <param name="timeProvider">Time source for the idle clock. Defaults to
    /// <see cref="TimeProvider.System"/>; override in tests.</param>
    /// <param name="cancellationToken">Cancels the start.</param>
    internal static async Task<SubscriptionHost> StartAsync(
        MessageConsumerSettings settings,
        MessageHandler handler,
        int? port,
        string componentName,
        ILoggerFactory loggerFactory,
        ActivityContext run = default,
        TimeProvider? timeProvider = null,
        CancellationToken cancellationToken = default)
    {
        var consumer = new MessageConsumer(settings, handler, componentName,
            loggerFactory.CreateLogger<MessageConsumer>(), timeProvider, run);
        var server = new MessageCallbackServer(consumer, port, loggerFactory);
        try
        {
            var listenPort = await server.StartAsync(cancellationToken);
            return new SubscriptionHost(consumer, server, listenPort);
        }
        catch
        {
            await server.DisposeAsync();
            consumer.Dispose();
            throw;
        }
    }

    /// <summary>Stops taking messages, drains those in flight (grace period, then interruption),
    /// and only then stops the callback — so the acks of messages in flight still reach the
    /// sidecar.</summary>
    /// <returns>How many messages were still in flight when the grace period ended.</returns>
    internal async Task<int> StopAsync(CancellationToken cancellationToken = default)
    {
        var unfinished = await Consumer.StopAsync();
        await _server.StopAsync(cancellationToken);
        return unfinished;
    }

    public async ValueTask DisposeAsync()
    {
        await _server.DisposeAsync();
        Consumer.Dispose();
    }
}
