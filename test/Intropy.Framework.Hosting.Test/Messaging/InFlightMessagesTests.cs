using Intropy.Framework.Hosting.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace Intropy.Framework.Hosting.Test.Messaging;

/// <summary>The messages in flight, the idle clock, and how a consumer stops.</summary>
public class InFlightMessagesTests
{
    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void IsIdle_IsFalse_WhileAMessageIsInFlight()
    {
        using var inFlight = new InFlightMessages(_time);
        Assert.True(inFlight.TryEnter());

        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.False(inFlight.IsIdle(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void IsIdle_IsFalse_BeforeTheIdleTimeout()
    {
        using var inFlight = new InFlightMessages(_time);

        _time.Advance(TimeSpan.FromSeconds(5));

        Assert.False(inFlight.IsIdle(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public void IsIdle_IsTrue_WhenNothingArrivedForTheIdleTimeoutAndNothingIsInFlight()
    {
        using var inFlight = new InFlightMessages(_time);
        Assert.True(inFlight.TryEnter());
        inFlight.Exit();

        _time.Advance(TimeSpan.FromSeconds(1));

        Assert.True(inFlight.IsIdle(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void IsIdle_MeasuresFromTheLastMessageThatArrived()
    {
        using var inFlight = new InFlightMessages(_time);
        _time.Advance(TimeSpan.FromSeconds(5));
        Assert.True(inFlight.TryEnter());
        inFlight.Exit();

        _time.Advance(TimeSpan.FromMilliseconds(500));

        Assert.False(inFlight.IsIdle(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void RestartIdleClock_MeasuresTheNextWindowFromTheRestart_NotFromConstruction()
    {
        using var inFlight = new InFlightMessages(_time);
        _time.Advance(TimeSpan.FromSeconds(10));

        inFlight.RestartIdleClock();

        _time.Advance(TimeSpan.FromMilliseconds(500));
        Assert.False(inFlight.IsIdle(TimeSpan.FromSeconds(1)));
        _time.Advance(TimeSpan.FromMilliseconds(500));
        Assert.True(inFlight.IsIdle(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void Count_TracksConcurrentMessages()
    {
        using var inFlight = new InFlightMessages(_time);

        Assert.True(inFlight.TryEnter());
        Assert.True(inFlight.TryEnter());
        Assert.Equal(2, inFlight.Count);
        inFlight.Exit();
        inFlight.Exit();

        Assert.Equal(0, inFlight.Count);
    }

    [Fact]
    public async Task StopAsync_WithNothingInFlight_ReturnsAtOnceAndTakesNoNewMessage()
    {
        using var inFlight = new InFlightMessages();

        var unfinished = await inFlight.StopAsync(TimeSpan.FromSeconds(10), NullLogger.Instance)
            .WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(0, unfinished);
        Assert.False(inFlight.TryEnter());
    }

    [Fact]
    public async Task StopAsync_WaitsForTheMessageInFlightToFinish()
    {
        using var inFlight = new InFlightMessages();
        Assert.True(inFlight.TryEnter());

        var stopping = inFlight.StopAsync(TimeSpan.FromSeconds(10), NullLogger.Instance);
        await Task.Delay(100);
        Assert.False(stopping.IsCompleted);
        inFlight.Exit();

        Assert.Equal(0, await stopping.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(inFlight.Interrupt.IsCancellationRequested);
    }

    [Fact]
    public async Task StopAsync_InterruptsAMessageOutlivingTheGracePeriodAndCountsIt()
    {
        using var inFlight = new InFlightMessages();
        Assert.True(inFlight.TryEnter());
        inFlight.Interrupt.Register(inFlight.Exit);

        var unfinished = await inFlight.StopAsync(TimeSpan.FromMilliseconds(100), NullLogger.Instance)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, unfinished);
        Assert.True(inFlight.Interrupt.IsCancellationRequested);
    }
}
