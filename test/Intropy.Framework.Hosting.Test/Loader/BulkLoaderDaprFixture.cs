namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A bulk loader behind a real Dapr sidecar: one subscription with a rule per event type and
/// <c>bulkSubscribe</c> on, as the topology renders a subscription declared <c>InBatches</c>.
/// </summary>
public sealed class BulkLoaderDaprFixture : DaprSidecarFixture
{
    /// <summary>How long the sidecar waits to fill a batch before it delivers what it has.</summary>
    public static readonly TimeSpan MaxAwait = TimeSpan.FromSeconds(1);

    public BulkLoaderHost Loader { get; private set; } = null!;

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
              - match: event.type == '{LoaderHost.Cancelled}'
                path: /{LoaderHost.Cancelled}
            default: /unhandled
          bulkSubscribe:
            enabled: true
            maxMessagesCount: 100
            maxAwaitDurationMs: {(int)MaxAwait.TotalMilliseconds}
        scopes:
          - {AppId}
        """;

    protected override async Task StartLoaderAsync(int appPort) =>
        Loader = await BulkLoaderHost.StartRoutingAsync(configure: o =>
        {
            o.CallbackPort = appPort;
            o.MaxBatchProcessingTime = TimeSpan.FromSeconds(10);
        });

    protected override ValueTask StopLoaderAsync() => Loader.DisposeAsync();
}
