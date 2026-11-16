namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A loader behind a real Dapr sidecar, its subscription's routing rules as the topology renders
/// them, one with a content filter.
/// </summary>
public class LoaderDaprFixture : DaprSidecarFixture
{
    /// <summary>The only cancellation reason the subscription's content filter selects.</summary>
    public const string HandledCancellationReason = "customer-request";

    public LoaderHost Loader { get; private set; } = null!;

    protected override string Subscription => $"""
        apiVersion: dapr.io/v2alpha1
        kind: Subscription
        metadata:
          name: {AppId}-orders
        spec:
          pubsubname: pubsub
          topic: {LoaderHost.Topic}
          routes:
            rules:
              - match: event.type == '{LoaderHost.Created}'
                path: /{LoaderHost.Created}
              - match: event.type == '{LoaderHost.Cancelled}' && (event.data.reason == '{HandledCancellationReason}')
                path: /{LoaderHost.Cancelled}
            default: /unhandled
        scopes:
          - {AppId}
        """;

    protected override async Task StartLoaderAsync(int appPort) =>
        Loader = await LoaderHost.StartRoutingAsync(o =>
        {
            o.CallbackPort = appPort;
            o.MaxMessageProcessingTime = TimeSpan.FromSeconds(10);
        });

    protected override ValueTask StopLoaderAsync() => Loader.DisposeAsync();
}

/// <summary>The same sidecar without the <c>Resiliency</c> policy, as a loader deployed without one
/// would run.</summary>
public sealed class LoaderDaprFixtureWithoutRetryPolicy : LoaderDaprFixture
{
    protected override bool WithRetryPolicy => false;
}
