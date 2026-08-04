using Intropy.Contracts.IdempotencyService;
using Intropy.Framework.Testing.Services;
using Action = Intropy.Contracts.IdempotencyService.Action;

namespace Intropy.Framework.Testing.Test.Services;

public class FakeIdempotencyServiceClientTests
{
    private static MessageInfo Info(string id = "msg-1") =>
        new("test-component", id, "hash", DateTimeOffset.UtcNow);

    [Fact]
    public async Task GetStatusAsync_Default_ProceedsWithNoPreviousData()
    {
        var client = new FakeIdempotencyServiceClient();

        var status = await client.GetStatusAsync(Info(), CancellationToken.None);

        Assert.Equal(Action.Proceed, status.Action);
        Assert.Equal(Reason.NoPreviousData, status.Reason);
    }

    [Fact]
    public async Task GetStatusAsync_StickyNextStatus_IsHonored()
    {
        var client = new FakeIdempotencyServiceClient
        {
            NextStatus = new StatusResponse(Action.Ignore, Reason.SameData),
        };

        var status = await client.GetStatusAsync(Info(), CancellationToken.None);

        Assert.Equal(Action.Ignore, status.Action);
        Assert.Equal(Reason.SameData, status.Reason);
    }

    [Fact]
    public async Task GetStatusAsync_QueuedStatuses_DequeueInOrder_ThenFallBackToNextStatus()
    {
        var client = new FakeIdempotencyServiceClient
        {
            NextStatus = new StatusResponse(Action.Ignore, Reason.SameData),
        };
        client.QueueStatus(
            new StatusResponse(Action.Proceed, Reason.NoPreviousData),
            new StatusResponse(Action.Proceed, Reason.NewerData));

        var first = await client.GetStatusAsync(Info(), CancellationToken.None);
        var second = await client.GetStatusAsync(Info(), CancellationToken.None);
        var third = await client.GetStatusAsync(Info(), CancellationToken.None);

        Assert.Equal(Reason.NoPreviousData, first.Reason);
        Assert.Equal(Reason.NewerData, second.Reason);
        Assert.Equal(Action.Ignore, third.Action);
    }

    [Fact]
    public async Task GetStatusAsync_RecordsStatusChecks()
    {
        var client = new FakeIdempotencyServiceClient();
        var info = Info();

        await client.GetStatusAsync(info, CancellationToken.None);

        var check = Assert.Single(client.StatusChecks);
        Assert.Same(info, check);
    }

    [Fact]
    public async Task CommitAsync_RecordsAndIsVisibleViaGetInfoAsync()
    {
        var client = new FakeIdempotencyServiceClient();
        var info = Info();

        Assert.Null(await client.GetInfoAsync("test-component", "msg-1", CancellationToken.None));

        await client.CommitAsync(info, CancellationToken.None);

        Assert.Same(info, Assert.Single(client.Committed));
        var stored = await client.GetInfoAsync("test-component", "msg-1", CancellationToken.None);
        Assert.Same(info, stored);
    }

    [Fact]
    public async Task GetInfoAsync_UncommittedPair_ReturnsNull()
    {
        var client = new FakeIdempotencyServiceClient();
        await client.CommitAsync(Info(), CancellationToken.None);

        var stored = await client.GetInfoAsync("test-component", "other-id", CancellationToken.None);

        Assert.Null(stored);
    }

    [Fact]
    public async Task StatusException_PropagatesAsTypedException_WithoutRecording()
    {
        var client = new FakeIdempotencyServiceClient
        {
            StatusException = new IdempotencyServiceException("service unavailable"),
        };

        await Assert.ThrowsAsync<IdempotencyServiceException>(
            () => client.GetStatusAsync(Info(), CancellationToken.None));
        Assert.Empty(client.StatusChecks);
    }
}
