using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Intropy.Framework.Hosting.Messaging.Callback;

/// <summary>
/// The Dapr app callback a loader serves under the app-callback transport. The loader announces no
/// subscription and no input binding: its subscription is a declarative Dapr <c>Subscription</c>
/// resource. Each message arrives as one <c>OnTopicEvent</c> call.
/// </summary>
internal sealed class LoaderAppCallbackService(LoaderCallbackDelivery delivery) : AppCallback.AppCallbackBase
{
    public override Task<ListTopicSubscriptionsResponse> ListTopicSubscriptions(Empty request,
        ServerCallContext context) => Task.FromResult(new ListTopicSubscriptionsResponse());

    public override Task<ListInputBindingsResponse> ListInputBindings(Empty request, ServerCallContext context) =>
        Task.FromResult(new ListInputBindingsResponse());

    public override Task<TopicEventResponse> OnTopicEvent(TopicEventRequest request, ServerCallContext context) =>
        delivery.HandleAsync(request, context.CancellationToken);
}
