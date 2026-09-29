using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Intropy.Framework.Hosting.Messaging.Bulk;

/// <summary>The Dapr app callback a batch loader serves: its subscription and its deliveries.</summary>
internal sealed class LoaderAppCallbackService(LoaderBulkDelivery delivery) : AppCallback.AppCallbackBase
{
    public override Task<ListTopicSubscriptionsResponse> ListTopicSubscriptions(Empty request,
        ServerCallContext context) => Task.FromResult(delivery.Subscriptions());

    public override Task<ListInputBindingsResponse> ListInputBindings(Empty request, ServerCallContext context) =>
        Task.FromResult(new ListInputBindingsResponse());

    public override Task<TopicEventBulkResponse> OnBulkTopicEvent(TopicEventBulkRequest request,
        ServerCallContext context) => delivery.HandleBulkAsync(request, context.CancellationToken);

    public override Task<TopicEventResponse> OnTopicEvent(TopicEventRequest request, ServerCallContext context) =>
        delivery.HandleSingleAsync(request, context.CancellationToken);
}
