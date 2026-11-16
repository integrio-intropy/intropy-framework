using System.Collections.Concurrent;
using Intropy.Framework.Blocks.BulkLoader;

namespace Intropy.Framework.Blocks.Test.BulkLoader;

public class BoundedParallelTests
{
    [Fact]
    public async Task RunAsync_PreservesInputOrder_RegardlessOfCompletionOrder()
    {
        // Earlier items sleep longer than later ones, so completion order is reversed.
        var items = Enumerable.Range(0, 10).ToList();

        var results = await BoundedParallel.RunAsync(
            items, maxDegreeOfParallelism: 10,
            async (item, _, ct) =>
            {
                await Task.Delay((10 - item) * 5, ct);
                return item;
            },
            CancellationToken.None);

        Assert.Equal(items, results);
    }

    [Fact]
    public async Task RunAsync_AtDegreeOne_RunsSequentially()
    {
        var items = Enumerable.Range(0, 5).ToList();
        var concurrentCount = 0;
        var maxObservedConcurrency = 0;
        var gate = new object();

        var results = await BoundedParallel.RunAsync(
            items, maxDegreeOfParallelism: 1,
            async (item, _, ct) =>
            {
                lock (gate)
                {
                    concurrentCount++;
                    maxObservedConcurrency = Math.Max(maxObservedConcurrency, concurrentCount);
                }
                await Task.Delay(5, ct);
                lock (gate) concurrentCount--;
                return item * 2;
            },
            CancellationToken.None);

        Assert.Equal(1, maxObservedConcurrency);
        Assert.Equal(items.Select(i => i * 2), results);
    }

    [Fact]
    public async Task RunAsync_HonorsTheConfiguredDegree()
    {
        var items = Enumerable.Range(0, 20).ToList();
        var concurrentCount = 0;
        var maxObservedConcurrency = 0;
        var gate = new object();

        await BoundedParallel.RunAsync(
            items, maxDegreeOfParallelism: 4,
            async (item, _, ct) =>
            {
                lock (gate)
                {
                    concurrentCount++;
                    maxObservedConcurrency = Math.Max(maxObservedConcurrency, concurrentCount);
                }
                await Task.Delay(20, ct);
                lock (gate) concurrentCount--;
                return item;
            },
            CancellationToken.None);

        Assert.True(maxObservedConcurrency <= 4, $"Expected at most 4 concurrent, observed {maxObservedConcurrency}");
    }

    [Fact]
    public async Task RunAsync_WithEmptyInput_ReturnsEmptyOutput()
    {
        var results = await BoundedParallel.RunAsync(
            Array.Empty<int>(), maxDegreeOfParallelism: 4,
            (item, _, _) => Task.FromResult(item), CancellationToken.None);

        Assert.Empty(results);
    }

    [Fact]
    public async Task RunAsync_PropagatesAnExceptionFromAnyItem()
    {
        var items = Enumerable.Range(0, 5).ToList();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            BoundedParallel.RunAsync<int, int>(
                items, maxDegreeOfParallelism: 4,
                (item, _, _) => item == 3 ? throw new InvalidOperationException("boom") : Task.FromResult(item),
                CancellationToken.None));
    }

    [Fact]
    public async Task RunAsync_StopsWhenCancelled()
    {
        var items = Enumerable.Range(0, 50).ToList();
        using var cts = new CancellationTokenSource();
        var bag = new ConcurrentBag<int>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            BoundedParallel.RunAsync(
                items, maxDegreeOfParallelism: 2,
                async (item, _, ct) =>
                {
                    if (item == 2) await cts.CancelAsync();
                    await Task.Delay(10, ct);
                    bag.Add(item);
                    return item;
                },
                cts.Token));
    }
}
