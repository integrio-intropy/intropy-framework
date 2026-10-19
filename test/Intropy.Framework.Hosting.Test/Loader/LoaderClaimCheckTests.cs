using CloudNative.CloudEvents;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Testing.Adapters;
using Intropy.Framework.Testing.Delivery;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// A loader resolving a claim-checked payload: a message carrying a <c>dataref</c> instead of its
/// payload has that payload fetched from the claim store before the pipeline runs, so no loader
/// pipeline — and no component's code — ever knows the mechanism exists.
/// </summary>
/// <remarks>
/// These drive the real gRPC app callback, so they cover the whole path the sidecar takes: the
/// pointer arrives as a string extension on the delivered event, exactly as it does in production.
/// The loader half of the claim check matters more than the Transactional Integration's: a loader
/// consumes from the system bus, where the publisher is another component's deployment.
/// </remarks>
public class LoaderClaimCheckTests
{
    private static readonly CloudEventAttribute s_dataRef =
        CloudEventAttribute.CreateExtension(CloudEventExtensionKeys.DataRef, CloudEventAttributeType.String);

    private const string Key = "orders/2026/03/07/msg-1";

    /// <summary>An event carrying a pointer and no payload at all, as a claim-checking publisher
    /// puts it on the wire.</summary>
    private static CloudEvent ClaimChecked(string dataRef) =>
        new()
        {
            Id = "msg-1",
            Source = new Uri("urn:test:source"),
            Type = LoaderHost.Created,
            Subject = "ORD-1",
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            [s_dataRef] = dataRef
        };

    private static CloudEvent Inline(object data) =>
        new()
        {
            Id = "msg-1",
            Source = new Uri("urn:test:source"),
            Type = LoaderHost.Created,
            Subject = "ORD-1",
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/json",
            Data = data
        };

    private static Action<IServiceCollection> WithStore(IFileAdapter store) =>
        services => services.AddKeyedSingleton(ClaimStoreServiceCollectionExtensions.ClaimStoreKey, store);

    [Fact]
    public async Task AClaimCheckedMessage_ReachesThePipelineWithThePayloadFromTheStore()
    {
        var store = new InMemoryFileAdapter().AddFile(Key, """{"orderId":"ORD-1","customer":"Ada"}""");
        await using var host = await LoaderHost.StartSingleAsync(extraServices: WithStore(store));

        var ack = await host.DeliverAsync(ClaimChecked(Key));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal(new OrderCreated("ORD-1", "Ada"), Assert.Single(host.CreatedSender.Sent).Value);
    }

    // The control for the test above: the same pipeline, the same store registered, a message that
    // simply carries its own payload. Registering a store must change nothing for inline messages.
    [Fact]
    public async Task AnInlineMessage_StillUsesItsOwnData()
    {
        var store = new InMemoryFileAdapter { ReadException = new InvalidOperationException("the store was read") };
        await using var host = await LoaderHost.StartSingleAsync(extraServices: WithStore(store));

        var ack = await host.DeliverAsync(Inline(new OrderCreated("ORD-2", "Grace")));

        Assert.Equal(DeliveryAck.Success, ack);
        Assert.Equal(new OrderCreated("ORD-2", "Grace"), Assert.Single(host.CreatedSender.Sent).Value);
    }

    // A fetch that fails is a technical failure, never a business one: the message is left for
    // redelivery and the broker dead-letters it once redelivery gives up. Reporting it as a business
    // incident would ack the message and strand the payload.
    [Fact]
    public async Task AMissingClaimedPayload_LeavesTheMessageForRedelivery()
    {
        await using var host = await LoaderHost.StartSingleAsync(
            extraServices: WithStore(new InMemoryFileAdapter()));

        var ack = await host.DeliverAsync(ClaimChecked(Key));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
        Assert.Empty(host.Incidents.Incidents);
    }

    // The shape of a mis-ordered rollout: a publisher claim-checking before its consumers can
    // resolve. It must fail saying so, rather than surfacing later as an empty payload.
    [Fact]
    public async Task APointerWithNoClaimStore_SaysSo()
    {
        await using var host = await LoaderHost.StartSingleAsync();

        var ack = await host.DeliverAsync(ClaimChecked(Key));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }

    // The loader's trust boundary is wider than the Transactional Integration's: anything able to
    // publish to the subscribed topic can set a dataref. The adapters join a key onto the store's
    // base path without normalising it, so a pointer that is not a claim key is never handed over.
    [Theory]
    [InlineData("/etc/passwd")]
    [InlineData("../../../etc/passwd")]
    [InlineData("orders/2026/03/07/../../../../secret")]
    [InlineData("orders\\2026\\03\\07\\msg-1")]
    [InlineData("orders/2026/3/7/msg-1")]
    public async Task APointerThatIsNotAClaimKey_IsNeverFetched(string dataRef)
    {
        // Any read at all blows up, so the assertion is that the store was never reached.
        var store = new InMemoryFileAdapter { ReadException = new InvalidOperationException("the store was read") };
        await using var host = await LoaderHost.StartSingleAsync(extraServices: WithStore(store));

        var ack = await host.DeliverAsync(ClaimChecked(dataRef));

        Assert.Equal(DeliveryAck.Retry, ack);
        Assert.Empty(host.CreatedSender.Sent);
    }
}
