using System.Diagnostics;
using System.Globalization;
using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Intropy.Framework.Hosting.Messaging;

/// <summary>
/// The Dapr gRPC app callback a subscribing block serves: a small Kestrel server (HTTP/2) inside
/// the block's own host, through which the sidecar pushes the messages of the block's subscription
/// to its <see cref="MessageConsumer"/>. The block announces no subscription and no input binding:
/// its subscription is a declarative Dapr <c>Subscription</c> resource. The sidecar needs
/// <c>app-port</c> and <c>app-protocol</c> <c>grpc</c>.
/// </summary>
/// <remarks>
/// A block consumes its subscription either one message at a time (<see cref="MessageConsumer"/>,
/// the <c>AppCallback</c> service) or in batches (<see cref="BulkMessageConsumer"/>, the
/// <c>AppCallbackAlpha</c> service) — which of the two the sidecar calls follows from the
/// subscription's own <c>bulkSubscribe</c>, not from anything the block says. The
/// <c>AppCallback</c> service is served either way: the sidecar asks it for the block's
/// programmatic subscriptions at startup, which are none, because the block's subscription is a
/// declarative resource.
/// </remarks>
internal sealed class MessageCallbackServer(
    MessageConsumer? consumer,
    BulkMessageConsumer? bulkConsumer,
    int? port,
    ILoggerFactory loggerFactory)
    : IAsyncDisposable
{
    private WebApplication? _server;

    /// <summary>The port this block's callback listens on: <paramref name="configured"/>, else
    /// <c>APP_PORT</c> from the environment, else 8080.</summary>
    internal static int ResolvePort(int? configured) =>
        configured
        ?? (int.TryParse(Environment.GetEnvironmentVariable("APP_PORT"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var appPort) ? appPort : 8080);

    /// <summary>Starts listening; returns the port.</summary>
    internal async Task<int> StartAsync(CancellationToken cancellationToken)
    {
        var listenPort = ResolvePort(port);
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.ListenAnyIP(listenPort, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(loggerFactory);
        builder.Services.AddSingleton(new AppCallbackService(consumer, bulkConsumer,
            loggerFactory.CreateLogger<MessageCallbackServer>()));
        builder.Services.AddGrpc();

        _server = builder.Build();
        _server.MapGrpcService<AppCallbackService>();
        await _server.StartAsync(cancellationToken);
        return listenPort;
    }

    /// <summary>Stops listening. Stop the consumer first, so the acks of messages in flight still
    /// reach the sidecar.</summary>
    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_server is not null)
            await _server.StopAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_server is not null)
            await _server.DisposeAsync();
    }

    private sealed class AppCallbackService(MessageConsumer? consumer, BulkMessageConsumer? bulkConsumer,
        ILogger logger) : AppCallback.AppCallbackBase
    {
        public override Task<ListTopicSubscriptionsResponse> ListTopicSubscriptions(Empty request,
            ServerCallContext context) => Task.FromResult(new ListTopicSubscriptionsResponse());

        public override Task<ListInputBindingsResponse> ListInputBindings(Empty request, ServerCallContext context) =>
            Task.FromResult(new ListInputBindingsResponse());

        public override Task<TopicEventResponse> OnTopicEvent(TopicEventRequest request, ServerCallContext context)
        {
            // A block that consumes in batches serves this callback only for the sidecar's startup
            // handshake: its subscription is bulk, so its messages arrive on the alpha callback. A
            // message here means the subscription is not the one the block was registered for.
            if (consumer is null)
            {
                logger.LogError(
                    "Message {MessageId} was pushed to the single-message callback of a block that consumes in batches; left for redelivery. Check the component's Subscription resource.",
                    request.Id);
                return Task.FromResult(new TopicEventResponse
                {
                    Status = TopicEventResponse.Types.TopicEventResponseStatus.Retry
                });
            }

            return consumer.HandleAsync(request, context.CancellationToken, DeliveryContext(context.RequestHeaders));
        }

        /// <summary>
        /// The callback the sidecar uses instead of <see cref="OnTopicEvent"/> for a subscription
        /// that declares <c>bulkSubscribe</c>: one call per batch, one acknowledgement per entry.
        /// The sidecar calls this stable method first and only falls back to the deprecated
        /// <c>OnBulkTopicEventAlpha1</c> if the app leaves it unimplemented, so the alpha service is
        /// not served at all.
        /// </summary>
        public override Task<TopicEventBulkResponse> OnBulkTopicEvent(TopicEventBulkRequest request,
            ServerCallContext context)
        {
            // A block that consumes one message at a time serves this callback only to answer the
            // sidecar: a batch here means its subscription declares bulkSubscribe and the block was
            // not registered for it.
            if (bulkConsumer is null)
            {
                logger.LogError(
                    "Batch {BatchId} of {Count} messages was pushed to a block that consumes one message at a time; left for redelivery. Either register it as a bulk loader or drop bulkSubscribe from its Subscription resource.",
                    request.Id, request.Entries.Count);
                var response = new TopicEventBulkResponse();
                foreach (var entry in request.Entries)
                    response.Statuses.Add(new TopicEventBulkResponseEntry
                    {
                        EntryId = entry.EntryId,
                        Status = TopicEventResponse.Types.TopicEventResponseStatus.Retry
                    });
                return Task.FromResult(response);
            }

            // Each entry carries its own trace context, which the batch's span links to. The call's
            // metadata is left unread: the sidecar sends a grpc-trace-bin header per entry, and
            // ServerCallContext.RequestHeaders throws decoding the repeated binary header.
            return bulkConsumer.HandleAsync(request, context.CancellationToken);
        }

        /// <summary>The trace context of the sidecar's span delivering the message, from the call's
        /// metadata; <see langword="default"/> when it sent none.</summary>
        private static ActivityContext DeliveryContext(Metadata headers) =>
            MessageActivity.TryParseTraceContext(headers.GetValue("traceparent"), headers.GetValue("tracestate"),
                out var context)
                ? context
                : default;
    }
}
