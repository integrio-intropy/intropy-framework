using Intropy.Framework.Blocks.BulkLoader;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Messaging;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>
/// The shapes a bulk loader's routes may take: one pipeline per CloudEvent type, or one for every
/// type — never both.
/// </summary>
public class BulkLoaderRoutesTests
{
    [Fact]
    public void On_ShouldRegisterARoutePerEventType()
    {
        var routes = new BulkLoaderRoutes()
            .On<string, string, Context>("order.created", Pipeline, Factory)
            .On<string, string, Context>("order.cancelled", Pipeline, Factory);

        Assert.Equal(["order.created", "order.cancelled"], routes.Routes.Select(r => r.Name));
        Assert.All(routes.Routes, route => Assert.False(route.IsCatchAll));
    }

    [Fact]
    public void On_WithTheSameEventTypeTwice_ShouldThrow()
    {
        var routes = new BulkLoaderRoutes().On<string, string, Context>("order.created", Pipeline, Factory);

        Assert.Throws<ArgumentException>(() =>
            routes.On<string, string, Context>("order.created", Pipeline, Factory));
    }

    [Fact]
    public void OnAny_ShouldRegisterOneCatchAllRoute()
    {
        var routes = new BulkLoaderRoutes().OnAny<string, string, Context>(Pipeline, Factory);

        Assert.True(Assert.Single(routes.Routes).IsCatchAll);
    }

    [Fact]
    public void OnAny_AfterATypedRoute_ShouldThrow()
    {
        var routes = new BulkLoaderRoutes().On<string, string, Context>("order.created", Pipeline, Factory);

        Assert.Throws<ArgumentException>(() => routes.OnAny<string, string, Context>(Pipeline, Factory));
    }

    [Fact]
    public void On_AfterACatchAllRoute_ShouldThrow()
    {
        var routes = new BulkLoaderRoutes().OnAny<string, string, Context>(Pipeline, Factory);

        Assert.Throws<ArgumentException>(() =>
            routes.On<string, string, Context>("order.created", Pipeline, Factory));
    }

    [Fact]
    public void OnAny_Twice_ShouldThrow()
    {
        var routes = new BulkLoaderRoutes().OnAny<string, string, Context>(Pipeline, Factory);

        Assert.Throws<ArgumentException>(() => routes.OnAny<string, string, Context>(Pipeline, Factory));
    }

    [Fact]
    public void RouteTable_WithACatchAllAmongTypedRoutes_ShouldThrow()
    {
        // The table enforces the same rule the DSL does, for routes built another way.
        var routes = new BulkLoaderRoutes()
            .On<string, string, Context>("order.created", Pipeline, Factory)
            .Routes;
        var catchAll = new BulkLoaderRoutes().OnAny<string, string, Context>(Pipeline, Factory).Routes;

        Assert.Throws<ArgumentException>(() => new BulkMessageRouteTable([.. routes, .. catchAll]));
    }

    [Fact]
    public void RouteTable_WithNoRoutes_ShouldThrow() =>
        Assert.Throws<ArgumentException>(() => new BulkMessageRouteTable([]));

    private static BulkLoaderBuilder<string, string, Context> Pipeline(
        BulkLoaderBuilder<string, string, Context> builder, IServiceProvider provider) => builder;

    private static Context Factory(Dictionary<string, string> metadata, bool isRetry) => new(metadata, isRetry);
}
