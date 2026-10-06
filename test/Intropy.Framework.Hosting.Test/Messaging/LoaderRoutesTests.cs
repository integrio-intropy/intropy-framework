using Intropy.Framework.Blocks.Loader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Loader;
using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// <see cref="LoaderRoutes.OnAny{TInput,TOutput,TCtx}"/>, the public catch-all route: through the
/// routed overload, a loader declares its one pipeline for every event type in the same block as
/// typed routes. One shape or the other — <see cref="LoaderRouteTable"/> rejects the mix, and
/// <c>OnAny</c> does already, at declaration.
/// </summary>
public class LoaderRoutesTests
{
    [Fact]
    public void OnAny_DeclaresTheCatchAll()
    {
        var routes = new LoaderRoutes().OnAny<int, int, Context>(Pipeline, ContextFactory);

        var route = Assert.Single(routes.Routes);
        Assert.Null(route.EventType); // the catch-all has no event type
        Assert.Equal("*", route.Name);
    }

    [Fact]
    public void OnAny_BuildsARouteTableThatHandlesEveryEventType()
    {
        var table = new LoaderRouteTable(new LoaderRoutes().OnAny<int, int, Context>(Pipeline, ContextFactory).Routes);

        Assert.False(table.IsRouting); // not routed by event type
        // Any event type is served by the same pipeline.
        Assert.Same(table.Find("order.created"), table.Find("order.cancelled"));
        Assert.Same(table.Routes[0], table.Find(null));
    }

    [Fact]
    public void OnAny_AfterTypedRoutes_IsRejected()
    {
        var routes = new LoaderRoutes();
        routes.On<int, int, Context>("order.created", Pipeline, ContextFactory);

        var error = Assert.Throws<ArgumentException>(() =>
            routes.OnAny<int, int, Context>(Pipeline, ContextFactory));

        Assert.Contains("cannot also have typed routes", error.Message);
    }

    [Fact]
    public void TypedRoutes_AfterOnAny_AreRejected()
    {
        var routes = new LoaderRoutes().OnAny<int, int, Context>(Pipeline, ContextFactory);

        var error = Assert.Throws<ArgumentException>(() =>
            routes.On<int, int, Context>("order.created", Pipeline, ContextFactory));

        Assert.Contains("cannot also have typed routes", error.Message);
    }

    [Fact]
    public void SecondOnAny_IsRejected()
    {
        var routes = new LoaderRoutes().OnAny<int, int, Context>(Pipeline, ContextFactory);

        var error = Assert.Throws<ArgumentException>(() =>
            routes.OnAny<int, int, Context>(Pipeline, ContextFactory));

        Assert.Contains("Every-event-type route", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OnAny_IsThePublicFormOfTheCatchAllTheRoutedOverloadBuilt() =>
        // The internal Any(...) helper the single-pipeline overload uses keeps working, and declares
        // the same shape as OnAny.
        Assert.Null(Assert.Single(LoaderRoutes.Any<int, int, Context>(Pipeline, ContextFactory).Routes).EventType);

    [Fact]
    public void RoutedOverload_AcceptsAnOnAnyRouteDeclaration()
    {
        var services = new ServiceCollection();

        // The catch-all declared through the routed overload registers like the single-pipeline
        // overload: one subscription, one pipeline per message.
        services.AddLoader(
            options =>
            {
                options.PubSubName = "pubsub";
                options.TopicName = "orders";
            },
            routes => routes.OnAny<int, int, Context>(Pipeline, ContextFactory));

        var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<MessageConsumerSettings>();
        Assert.Equal("pubsub", settings.PubSubName);
        Assert.Equal("orders", settings.TopicName);
    }

    private static LoaderBuilder<int, int, Context> Pipeline(LoaderBuilder<int, int, Context> builder,
        IServiceProvider _) => builder;

    private static ContextFactory<Context> ContextFactory => (metadata, isRetry) => new Context(metadata, isRetry);
}
