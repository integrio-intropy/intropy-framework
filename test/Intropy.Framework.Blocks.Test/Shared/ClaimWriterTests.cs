using System.Diagnostics;
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Core.Configuration;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Intropy.Framework.Blocks.Test.Shared;

/// <summary>
/// The claim check's writing sequence, which both producing blocks share: refuse a name the key
/// format cannot hold, mint the key, tag the trace, write the payload.
/// </summary>
/// <remarks>
/// These exist because the sequence used to be written twice. Pinning it here is what makes the
/// next hardening land in both blocks rather than one, which is how the id check came to be missing
/// from the Transactional Integration in the first place.
/// </remarks>
public class ClaimWriterTests
{
    private static readonly DateTimeOffset Now = new(2026, 3, 7, 11, 0, 0, TimeSpan.Zero);

    private static readonly FrameworkOptions Options =
        new() { ComponentName = "orders", ServiceNamespace = "example" };

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static Task<string> WriteAsync(IFileAdapter store, string messageId, byte[]? payload = null,
        FrameworkOptions? options = null) =>
        ClaimWriter.WriteAsync(store, options ?? Options, new FixedTimeProvider(Now), messageId,
            payload ?? [1, 2, 3], CancellationToken.None);

    [Fact]
    public async Task WriteAsync_FilesThePayloadUnderAKeyNamingTheMessage()
    {
        var store = Substitute.For<IFileAdapter>();
        byte[] payload = [1, 2, 3, 4];

        var key = await WriteAsync(store, "msg-1", payload);

        Assert.Equal("orders/2026/03/07/msg-1", key);
        await store.Received(1).WriteAsync(key, payload, null, Arg.Any<CancellationToken>());
    }

    // The key is handed to a file adapter that joins it onto a base path without normalising, so a
    // reader has to accept what a writer mints — the two halves of one format.
    [Fact]
    public async Task WriteAsync_MintsAKeyItsOwnReaderAccepts()
    {
        var key = await WriteAsync(Substitute.For<IFileAdapter>(), Guid.NewGuid().ToString());

        Assert.True(ClaimKey.IsValid(key));
    }

    [Fact]
    public async Task WriteAsync_RefusesAMessageIdThatCannotNameAKey_NamingIt()
    {
        var store = Substitute.For<IFileAdapter>();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(store, "order:42"));

        Assert.Contains("order:42", error.Message, StringComparison.Ordinal);
        Assert.Contains("message id", error.Message, StringComparison.Ordinal);
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    // The component name is configuration rather than something the framework mints, so it is the
    // likelier of the two segments to arrive unusable.
    [Fact]
    public async Task WriteAsync_RefusesAComponentNameThatCannotNameAKey_NamingIt()
    {
        var store = Substitute.For<IFileAdapter>();
        var options = new FrameworkOptions { ComponentName = "orders/eu", ServiceNamespace = "example" };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            WriteAsync(store, "msg-1", options: options));

        Assert.Contains("orders/eu", error.Message, StringComparison.Ordinal);
        Assert.Contains("component name", error.Message, StringComparison.Ordinal);
        await store.DidNotReceiveWithAnyArgs().WriteAsync(default!, default!, default, default);
    }

    [Theory]
    [InlineData("urn:uuid:7c3f1a90")]
    [InlineData("order/42")]
    [InlineData("..")]
    [InlineData(".")]
    [InlineData("")]
    public void RequireCanName_RefusesWhatTheKeyFormatCannotHold(string messageId)
    {
        Assert.Throws<InvalidOperationException>(() => ClaimWriter.RequireCanName(Options, messageId));
    }

    // The control: the shapes the framework actually mints, plus the punctuation the format allows.
    [Theory]
    [InlineData("7c3f1a90-0f4e-4a1e-9a7e-2f3c5d6e7f80")]
    [InlineData("order.42")]
    [InlineData("order_42")]
    public void RequireCanName_AcceptsWhatTheKeyFormatHolds(string messageId)
    {
        ClaimWriter.RequireCanName(Options, messageId);
    }

    // The component name is deployment configuration rather than something the framework mints, so a
    // caller configuring a claim check can be told now — when the name is already known and cannot
    // change — instead of on the first payload large enough to be filed.
    [Theory]
    [InlineData("orders/eu")]
    [InlineData("orders eu")]
    [InlineData("..")]
    public void RequireComponentCanName_RefusesANameTheKeyFormatCannotHold(string componentName)
    {
        var options = new FrameworkOptions { ComponentName = componentName, ServiceNamespace = "example" };

        var error = Assert.Throws<InvalidOperationException>(() => ClaimWriter.RequireComponentCanName(options));

        Assert.Contains(componentName, error.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(FrameworkOptions.ComponentName), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RequireComponentCanName_AcceptsTheNamesComponentsActuallyUse()
    {
        ClaimWriter.RequireComponentCanName(
            new FrameworkOptions { ComponentName = "orders-integration", ServiceNamespace = "example" });
    }

    // Writing the payload before anything publishes a pointer to it is what makes a crash leave an
    // orphan rather than a pointer to nothing — so a failed write must escape, not be swallowed.
    [Fact]
    public async Task WriteAsync_LetsAFailedWriteEscape()
    {
        var store = Substitute.For<IFileAdapter>();
        store.WriteAsync(default!, default!, default, default)
            .ThrowsAsyncForAnyArgs(new InvalidOperationException("the store is unreachable"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(store, "msg-1"));
    }

    // The store write is I/O that can be slow or fail. Without a span of its own it hides inside the
    // enqueue or send it precedes, leaving a slow claim store indistinguishable from a slow broker.
    [Fact]
    public async Task WriteAsync_GivesTheStoreWriteItsOwnSpan()
    {
        var started = new List<Activity>();
        using var listener = Listen(started);
        using var step = new ActivitySource("Intropy.Framework.Blocks.Test").StartActivity("Step.Enqueue")!;

        var key = await WriteAsync(Substitute.For<IFileAdapter>(), "msg-1", [1, 2, 3, 4]);

        // Filtered by parent, not just by name: the listener sees the whole Blocks source, and other
        // test classes in this assembly run in parallel and start ClaimCheck spans of their own.
        var claimCheck = Assert.Single(started, a => a.ParentSpanId == step.SpanId);
        Assert.Equal("ClaimCheck", claimCheck.OperationName);
        Assert.Equal(key, claimCheck.GetTagItem("intropy.claim_check.key"));
    }

    // Nothing happens on the inline path, so a span for it would be noise — but the tag still has to
    // be there, or "was this claim-checked?" has no answer.
    [Fact]
    public void Inline_StartsNoSpan()
    {
        var started = new List<Activity>();
        using var listener = Listen(started);
        using var step = new ActivitySource("Intropy.Framework.Blocks.Test").StartActivity("Step.Enqueue")!;

        ClaimWriter.Inline(42);

        // By parent, for the same reason as above: a ClaimCheck span from a concurrent test class
        // would otherwise fail this.
        Assert.DoesNotContain(started, a => a.ParentSpanId == step.SpanId);
        Assert.Equal(false, step.GetTagItem("intropy.claim_check"));
    }

    [Fact]
    public async Task WriteAsync_TagsTheTraceWithTheClaimAndItsKey()
    {
        using var listener = Listen();
        using var activity = new ActivitySource("Intropy.Framework.Blocks.Test").StartActivity("probe")!;

        var key = await WriteAsync(Substitute.For<IFileAdapter>(), "msg-1", [1, 2, 3, 4]);

        Assert.Equal(true, activity.GetTagItem("intropy.claim_check"));
        Assert.Equal(4, activity.GetTagItem("intropy.claim_check.payload_size"));
        Assert.Equal(key, activity.GetTagItem("intropy.claim_check.key"));
    }

    // The inline path says so rather than leaving the trace silent, so "was this claim-checked?" has
    // an answer either way.
    [Fact]
    public void Inline_TagsTheTraceWithoutAKey()
    {
        using var listener = Listen();
        using var activity = new ActivitySource("Intropy.Framework.Blocks.Test").StartActivity("probe")!;

        ClaimWriter.Inline(42);

        Assert.Equal(false, activity.GetTagItem("intropy.claim_check"));
        Assert.Equal(42, activity.GetTagItem("intropy.claim_check.payload_size"));
        Assert.Null(activity.GetTagItem("intropy.claim_check.key"));
    }

    /// <summary>Makes <see cref="Activity.Current"/> non-null, which it is not without a listener,
    /// and collects the spans the writer starts. Those come from the <c>Intropy.Framework.Blocks</c>
    /// source rather than the Core one the pipeline's <c>Step.*</c> spans use.</summary>
    private static ActivityListener Listen(List<Activity>? started = null)
    {
        var listener = new ActivityListener
        {
            ActivityStarted = a => started?.Add(a),
            ShouldListenTo = source =>
                source.Name is "Intropy.Framework.Blocks.Test" or "Intropy.Framework.Blocks",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
