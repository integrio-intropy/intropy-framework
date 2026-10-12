using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Send;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

/// <summary>Builds a Transactional Integration's send-side handler the way the registration does,
/// for tests that drive the send side directly.</summary>
internal static class SendRouting
{
    /// <summary>The handler of an integration without routes: one send pipeline for every message.</summary>
    internal static RoutingMessageHandler Handler<TCtx>(ISendPipeline<TCtx> pipeline, ContextFactory<TCtx> contextFactory)
        where TCtx : Context =>
        Handler(new MessageRouteTable([new RegisteredSendRoute<TCtx>(contextFactory)]),
            services => services.AddSingleton(pipeline));

    /// <summary>The handler of an integration with <paramref name="routes"/>.</summary>
    internal static RoutingMessageHandler Handler(MessageRouteTable routes,
        Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        configure?.Invoke(services);
        return new RoutingMessageHandler(services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            routes, "test-integration", _ => "no send route matches", defaultPathIsUnrouted: false);
    }
}
