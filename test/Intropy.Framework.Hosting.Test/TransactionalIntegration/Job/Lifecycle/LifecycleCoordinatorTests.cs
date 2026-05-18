using Intropy.Framework.Hosting.TransactionalIntegration.Job.Lifecycle;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration.Job.Lifecycle;

public class LifecycleCoordinatorTests
{
    [Fact]
    public async Task PublishingCompleteSignal_ShouldComplete_WhenSignalPublishingCompleteIsCalled()
    {
        // Verifies that the publishing complete signal is successfully triggered
        var coordinator = new LifecycleCoordinator();

        var task = coordinator.PublishingCompleteSignal;
        Assert.False(task.IsCompleted);

        coordinator.SignalPublishingComplete();

        await task;
        Assert.True(task.IsCompleted);
        Assert.True(await task);
    }

    [Fact]
    public async Task PublishingCompleteSignal_ShouldFault_WhenSignalPublishingFailedIsCalled()
    {
        // Verifies that publishing failures are propagated to waiting subscribers
        var coordinator = new LifecycleCoordinator();
        var expectedException = new IOException("Publishing failed");

        var task = coordinator.PublishingCompleteSignal;
        Assert.False(task.IsCompleted);

        coordinator.SignalPublishingFailed(expectedException);

        var exception = await Assert.ThrowsAsync<IOException>(async () => await task);
        Assert.Equal("Publishing failed", exception.Message);
    }

    [Fact]
    public void SignalPublishingComplete_ShouldBeIdempotent()
    {
        // Verifies that signaling completion multiple times doesn't cause errors
        var coordinator = new LifecycleCoordinator();

        coordinator.SignalPublishingComplete();
        coordinator.SignalPublishingComplete();
        coordinator.SignalPublishingComplete();

        Assert.True(coordinator.PublishingCompleteSignal.IsCompleted);
    }

    [Fact]
    public async Task SignalPublishingFailed_ShouldOnlySetFirstException()
    {
        // Verifies that only the first failure exception is preserved when multiple failures occur
        var coordinator = new LifecycleCoordinator();
        var firstException = new InvalidOperationException("First failure");
        var secondException = new InvalidOperationException("Second failure");

        coordinator.SignalPublishingFailed(firstException);
        coordinator.SignalPublishingFailed(secondException);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await coordinator.PublishingCompleteSignal);
        Assert.Equal("First failure", exception.Message);
    }

    [Fact]
    public async Task PublishingCompleteSignal_ShouldBeAwaitableByMultipleConsumers()
    {
        // Verifies that multiple components can wait for the same publishing complete signal
        var coordinator = new LifecycleCoordinator();

        var task1 = coordinator.PublishingCompleteSignal;
        var task2 = coordinator.PublishingCompleteSignal;
        var task3 = coordinator.PublishingCompleteSignal;

        coordinator.SignalPublishingComplete();

        await Task.WhenAll(task1, task2, task3);

        Assert.True(task1.IsCompleted);
        Assert.True(task2.IsCompleted);
        Assert.True(task3.IsCompleted);
    }
}
