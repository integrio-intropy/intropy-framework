using System.Threading.Channels;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Hosting.Jobs;
using Intropy.Framework.Hosting.TransactionalIntegration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace Intropy.Framework.Hosting.Test.TransactionalIntegration;

public class InternalQueueReadinessGateTests
{
    private readonly FakeTimeProvider _time = new();
    private readonly RecordingProbe _probe = new();

    private InternalQueueReadinessGate Gate() => new(_probe, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(1),
        NullLogger<InternalQueueReadinessGate>.Instance, _time);

    [Fact]
    public async Task WaitAsync_OnlyAnIssuedProbeMakesTheRunReady()
    {
        var readinessCheck = Gate().CreateCheck();
        var ready = readinessCheck.WaitAsync(CancellationToken.None);
        var id = await _probe.NextAsync();

        readinessCheck.Delivered("stale-probe");
        Assert.False(ready.IsCompleted);

        readinessCheck.Delivered(id);
        await ready.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task WaitAsync_RetriesWithFreshIds_AndAcceptsALateEarlierAttempt()
    {
        var readinessCheck = Gate().CreateCheck();
        var ready = readinessCheck.WaitAsync(CancellationToken.None);
        var first = await _probe.NextAsync();
        _time.Advance(TimeSpan.FromSeconds(1));
        var second = await _probe.NextAsync();

        Assert.NotEqual(first, second);
        Assert.False(ready.IsCompleted);
        readinessCheck.Delivered(first);
        await ready.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task WaitAsync_AProbeFromAnEarlierRunDoesNotMakeANewRunReady()
    {
        var gate = Gate();
        var previous = gate.CreateCheck();
        var previousReady = previous.WaitAsync(CancellationToken.None);
        var previousId = await _probe.NextAsync();
        previous.Delivered(previousId);
        await previousReady;

        var current = gate.CreateCheck();
        var ready = current.WaitAsync(CancellationToken.None);
        var currentId = await _probe.NextAsync();
        current.Delivered(previousId);
        Assert.False(ready.IsCompleted);

        current.Delivered(currentId);
        await ready.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task WaitAsync_DeadlineFailsAsInfrastructure()
    {
        var readinessCheck = Gate().CreateCheck();
        var ready = readinessCheck.WaitAsync(CancellationToken.None);
        await _probe.NextAsync();
        _time.Advance(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAsync<InfrastructureUnavailableException>(() => ready);
    }

    [Fact]
    public async Task WaitAsync_HostCancellationIsNotAnInfrastructureFailure()
    {
        using var cancellation = new CancellationTokenSource();
        var readinessCheck = Gate().CreateCheck();
        var ready = readinessCheck.WaitAsync(cancellation.Token);
        await _probe.NextAsync();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ready);
    }

    private sealed class RecordingProbe : IInternalQueueProbe
    {
        private readonly Channel<string> _ids = Channel.CreateUnbounded<string>();

        public Task PublishProbeAsync(string probeId, CancellationToken ct) =>
            _ids.Writer.WriteAsync(probeId, ct).AsTask();

        public Task<string> NextAsync() =>
            _ids.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }
}
