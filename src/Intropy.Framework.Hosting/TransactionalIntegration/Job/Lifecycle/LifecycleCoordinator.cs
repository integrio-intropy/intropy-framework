namespace Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

internal class LifecycleCoordinator
{
    private readonly TaskCompletionSource<bool> _publishingComplete = new();

    internal Task<bool> PublishingCompleteSignal => _publishingComplete.Task;

    internal void SignalPublishingComplete() => _publishingComplete.TrySetResult(true);

    internal void SignalPublishingFailed(Exception ex) => _publishingComplete.TrySetException(ex);
}
