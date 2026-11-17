using Intropy.Framework.Hosting.StatefulSweeps;
using Intropy.Framework.Testing.StatefulSweeps;

namespace Intropy.Framework.Testing.Test.StatefulSweeps;

/// <summary>
/// The cursor store fake fails like the Dapr store: a save must carry the ETag it read or last
/// saved, a record someone else wrote in between fails the save without changing it, and a failing
/// store saves nothing.
/// </summary>
public class InMemoryCursorStoreTests
{
    private const string Key = "component:port";

    [Fact]
    public async Task ASave_ReturnsTheETagTheNextSaveMustCarry()
    {
        var store = new InMemoryCursorStore();

        var first = await store.SaveAsync(Key, new CursorState("c0", "c1"), null, CancellationToken.None);
        var second = await store.SaveAsync(Key, new CursorState("c1", null), first, CancellationToken.None);

        Assert.NotEqual(first, second);
        var stored = await store.GetAsync(Key, CancellationToken.None);
        Assert.Equal(new StoredCursor(new CursorState("c1", null), second), stored);
        Assert.Equal([new CursorState("c0", "c1"), new CursorState("c1", null)], store.Saves);
    }

    [Fact]
    public async Task ASaveWithAStaleETag_Fails_AndKeepsTheRecord()
    {
        var store = new InMemoryCursorStore();
        store.Set(Key, new CursorState("c0", null));
        var read = await store.GetAsync(Key, CancellationToken.None);

        store.Set(Key, new CursorState("c5", null));

        await Assert.ThrowsAsync<CursorConflictException>(() =>
            store.SaveAsync(Key, new CursorState("c1", null), read!.ETag, CancellationToken.None));
        Assert.Equal(new CursorState("c5", null), store.State[Key]);
        Assert.Empty(store.Saves);
    }

    [Fact]
    public async Task ASaveWithoutAnETag_FailsWhenARecordWasCreatedMeanwhile() =>
        await Assert.ThrowsAsync<CursorConflictException>(() =>
        {
            var store = new InMemoryCursorStore();
            store.Set(Key, new CursorState("c0", null));
            return store.SaveAsync(Key, new CursorState("c1", null), null, CancellationToken.None);
        });

    [Fact]
    public async Task AFailingStore_SavesNothing()
    {
        var store = new InMemoryCursorStore { SaveFailure = _ => new IOException("unreachable") };

        await Assert.ThrowsAsync<IOException>(() =>
            store.SaveAsync(Key, new CursorState("c0", null), null, CancellationToken.None));

        Assert.Empty(store.State);
        Assert.Empty(store.Saves);
    }
}
