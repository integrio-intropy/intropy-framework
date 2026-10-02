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
internal sealed class MessageCallbackServer(MessageConsumer consumer, int? port, ILoggerFactory loggerFactory)
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
        builder.Services.AddSingleton(consumer);
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

    private sealed class AppCallbackService(MessageConsumer consumer) : AppCallback.AppCallbackBase
    {
        public override Task<ListTopicSubscriptionsResponse> ListTopicSubscriptions(Empty request,
            ServerCallContext context) => Task.FromResult(new ListTopicSubscriptionsResponse());

        public override Task<ListInputBindingsResponse> ListInputBindings(Empty request, ServerCallContext context) =>
            Task.FromResult(new ListInputBindingsResponse());

        public override Task<TopicEventResponse> OnTopicEvent(TopicEventRequest request, ServerCallContext context) =>
            consumer.HandleAsync(request, context.CancellationToken);
    }
}
