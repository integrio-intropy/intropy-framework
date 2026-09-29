using Dapr.AppCallback.Autogen.Grpc.v1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Intropy.Framework.Hosting.Test.Loader;

/// <summary>
/// Calls an app's Dapr gRPC app callback the way the sidecar does. Dapr.Protos ships only the
/// server side of the AppCallback service, so the calls are described from its service descriptor.
/// </summary>
public sealed class AppCallbackTestClient(CallInvoker invoker)
{
    private static readonly string Service = AppCallback.Descriptor.FullName;

    public Task<ListTopicSubscriptionsResponse> ListTopicSubscriptionsAsync() =>
        CallAsync("ListTopicSubscriptions", new Empty(), ListTopicSubscriptionsResponse.Parser);

    public Task<TopicEventResponse> OnTopicEventAsync(TopicEventRequest request) =>
        CallAsync("OnTopicEvent", request, TopicEventResponse.Parser);

    public Task<TopicEventBulkResponse> OnBulkTopicEventAsync(TopicEventBulkRequest request) =>
        CallAsync("OnBulkTopicEvent", request, TopicEventBulkResponse.Parser);

    private async Task<TResponse> CallAsync<TRequest, TResponse>(string name, TRequest request,
        MessageParser<TResponse> parser)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse>
    {
        var method = new Method<TRequest, TResponse>(MethodType.Unary, Service, name,
            Marshallers.Create<TRequest>(r => r.ToByteArray(), _ => throw new NotSupportedException()),
            Marshallers.Create(r => r.ToByteArray(), parser.ParseFrom));
        return await invoker.AsyncUnaryCall(method, null, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(30)),
            request);
    }
}
