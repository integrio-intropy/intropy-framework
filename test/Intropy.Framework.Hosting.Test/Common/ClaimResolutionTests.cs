using System.Text;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Hosting.Common;
using Intropy.Framework.Hosting.Messaging;
using Intropy.Framework.Hosting.Test.Messaging;
using Intropy.Framework.Testing.Adapters;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Test.Common;

/// <summary>
/// Choosing a message's payload: the claim check's consuming half, shared by the loader and the
/// Transactional Integration's send route. These pin the wording of the two refusals, because both
/// name a deployment mistake an operator has to recognise from the message alone.
/// </summary>
public class ClaimResolutionTests
{
    private const string Key = "orders/2026/03/07/msg-1";

    private static IncomingMessage Message(string? dataRef) =>
        IncomingMessage.From(MessageConsumerTests.Request("msg-1", extensions: dataRef is null
            ? null
            : new Dictionary<string, string> { [CloudEventExtensionKeys.DataRef] = dataRef }));

    private static ServiceProvider Scope(InMemoryFileAdapter? store)
    {
        var services = new ServiceCollection();
        if (store is not null)
            services.AddKeyedSingleton<IFileAdapter>(ClaimStoreServiceCollectionExtensions.ClaimStoreKey, store);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task ResolveAsync_FetchesTheClaimedPayload_WhenTheMessageCarriesAPointer()
    {
        var store = new InMemoryFileAdapter().AddFile(Key, """{"orderId":"ORD-1"}""");

        var payload = await ClaimResolution.ResolveAsync(Message(Key), Scope(store), CancellationToken.None);

        Assert.Equal("""{"orderId":"ORD-1"}""", Encoding.UTF8.GetString(payload.Span));
    }

    [Fact]
    public async Task ResolveAsync_UsesTheMessagesOwnData_WhenItCarriesNoPointer()
    {
        var store = new InMemoryFileAdapter { ReadException = new InvalidOperationException("the store was read") };

        var payload = await ClaimResolution.ResolveAsync(Message(null), Scope(store), CancellationToken.None);

        Assert.Equal(Message(null).Data.ToArray(), payload.ToArray());
    }

    // The shape of a mis-ordered rollout — a publisher claim-checking before its consumers can
    // resolve — so the message has to say that, not surface later as an empty payload.
    [Fact]
    public async Task ResolveAsync_NamesTheMissingClaimStore_WhenAPointerArrivesWithNoneRegistered()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ClaimResolution.ResolveAsync(Message(Key), Scope(null), CancellationToken.None));

        Assert.Contains("no claim store is registered", error.Message, StringComparison.Ordinal);
        Assert.Contains("AddClaimStore", error.Message, StringComparison.Ordinal);
    }

    // Checked before the store is even resolved, so a malformed pointer is refused the same way
    // whether or not the component could have fetched it.
    [Theory]
    [InlineData("/etc/passwd")]
    [InlineData("../../../etc/passwd")]
    [InlineData("orders/2026/03/07/../../../../secret")]
    [InlineData("orders\\2026\\03\\07\\msg-1")]
    [InlineData("orders/2026/3/7/msg-1")]
    public async Task ResolveAsync_RefusesAPointerThatIsNotAClaimKey_WithoutReadingTheStore(string dataRef)
    {
        var store = new InMemoryFileAdapter { ReadException = new InvalidOperationException("the store was read") };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ClaimResolution.ResolveAsync(Message(dataRef), Scope(store), CancellationToken.None));

        Assert.Contains("is not a claim key", error.Message, StringComparison.Ordinal);
    }
}
