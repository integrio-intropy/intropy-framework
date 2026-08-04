using Dapr.Client;

namespace Intropy.Framework.Testing.Dapr;

/// <summary>
/// Captures pub/sub publish calls made through a <see cref="DaprClient"/>, deleting the brittle
/// NSubstitute call-capture plumbing (<c>ReceivedCalls()</c> filtering, manual argument indexing,
/// <c>JsonEventFormatter</c> decoding) that component publish tests otherwise hand-roll.
/// </summary>
/// <remarks>
/// <para>
/// This is a capture helper, not a full <see cref="DaprClient"/> fake: the client substitute is
/// still configured by the consuming test project (one line per fake — see the example). The
/// capture works with any mocking framework, since it only consumes the recorded argument values.
/// </para>
/// <para>
/// Assert on <see cref="Messages"/> directly, or decode envelopes via
/// <see cref="PublishedMessage.DecodeCloudEvent"/> to assert on the exact CloudEvent the real
/// sidecar would have received.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var daprClient = Substitute.For&lt;DaprClient&gt;();
/// var capture = new PublishedMessageCapture();
/// daprClient
///     .PublishByteEventAsync(
///         Arg.Any&lt;string&gt;(), Arg.Any&lt;string&gt;(), Arg.Any&lt;ReadOnlyMemory&lt;byte&gt;&gt;(),
///         Arg.Any&lt;string?&gt;(), Arg.Any&lt;Dictionary&lt;string, string&gt;?&gt;(), Arg.Any&lt;CancellationToken&gt;())
///     .Returns(Task.CompletedTask)
///     .AndDoes(ci =&gt; capture.Capture(
///         ci.Arg&lt;string&gt;(), ci.ArgAt&lt;string&gt;(1), ci.ArgAt&lt;ReadOnlyMemory&lt;byte&gt;&gt;(2),
///         ci.ArgAt&lt;string?&gt;(3)));
///
/// // ... run the pipeline ...
///
/// var message = capture.Messages.Single();
/// Assert.Equal("orders", message.TopicName);
/// Assert.Equal("order-42", message.DecodeCloudEvent().Subject);
/// </code>
/// </example>
public sealed class PublishedMessageCapture
{
    private readonly List<PublishedMessage> _messages = [];
    private readonly object _lock = new();

    /// <summary>
    /// Gets the captured publish calls in publication order, as a snapshot.
    /// </summary>
    public IReadOnlyList<PublishedMessage> Messages
    {
        get
        {
            lock (_lock)
            {
                return [.. _messages];
            }
        }
    }

    /// <summary>
    /// Gets the number of captured publish calls.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _messages.Count;
            }
        }
    }

    /// <summary>
    /// Records a <c>PublishByteEventAsync</c> call. Typically wired into the mocking framework's
    /// call callback (see the class example).
    /// </summary>
    /// <param name="pubSubName">The pub/sub component name argument.</param>
    /// <param name="topicName">The topic name argument.</param>
    /// <param name="data">The payload argument.</param>
    /// <param name="contentType">The content type argument, if any.</param>
    public void Capture(string pubSubName, string topicName, ReadOnlyMemory<byte> data, string? contentType = null)
    {
        lock (_lock)
        {
            _messages.Add(new PublishedMessage(pubSubName, topicName, contentType)
            {
                Data = data.ToArray(),
            });
        }
    }
}
