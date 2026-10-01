namespace Intropy.Framework.Hosting.Loader;

/// <summary>How the sidecar delivers a loader's messages.</summary>
/// <remarks>
/// Experimental: both transports exist while the app callback is evaluated. Only one will ship, and
/// this choice goes away with the other.
/// </remarks>
public enum LoaderTransport
{
    /// <summary>A Dapr streaming subscription (the default): the loader connects to its sidecar and
    /// serves nothing, so it needs no app port.</summary>
    Streaming,

    /// <summary>The Dapr gRPC app callback: the sidecar pushes each message to a gRPC server the loader
    /// runs on <see cref="LoaderOptions.CallbackPort"/>. The subscription is a declarative Dapr
    /// <c>Subscription</c> resource; the loader announces none. The sidecar needs <c>app-port</c> and
    /// <c>app-protocol</c> <c>grpc</c>.</summary>
    AppCallback
}
