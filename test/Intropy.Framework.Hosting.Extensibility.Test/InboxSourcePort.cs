using System.Runtime.CompilerServices;
using System.Text;
using Intropy.Framework.Hosting.Sweeps;

namespace Acme.Adapters.Inbox;

/// <summary>
/// An external system's inbox of messages, each acknowledged once handled: what an adapter
/// package would reach over the network (a queue, a polled API). In memory here, with a way to
/// make its listing fail partway, as a network listing can.
/// </summary>
public sealed class Inbox
{
    private readonly List<InboxMessage> _messages = [];

    public IReadOnlyList<InboxMessage> Pending => _messages;

    /// <summary>How many times the inbox was listed.</summary>
    public int Listings { get; private set; }

    /// <summary>When set, the failure thrown instead of listing the message at this index.</summary>
    public Func<int, Exception?>? FailBeforeMessage { get; set; }

    /// <summary>When set, replaces how a message's identity is resolved: an adapter's mapping that
    /// can throw or find no identity.</summary>
    public Func<InboxMessage, string?>? IdOf { get; set; }

    /// <summary>When set, replaces what a message adds to its telemetry.</summary>
    public Func<InboxMessage, SweepItemTelemetry>? TelemetryOf { get; set; }

    public Inbox Add(string id, string body, string batch = "b-1")
    {
        _messages.Add(new InboxMessage(id, body, batch));
        return this;
    }

    public IReadOnlyList<InboxMessage> List() => [.. _messages];

    public async IAsyncEnumerable<InboxMessage> ListAsync([EnumeratorCancellation] CancellationToken ct)
    {
        Listings++;
        var index = 0;
        foreach (var message in _messages.ToList())
        {
            if (FailBeforeMessage?.Invoke(index) is { } failure)
                throw failure;

            await Task.Yield();
            yield return message;
            index++;
        }
    }

    public void Acknowledge(string id) => _messages.RemoveAll(m => m.Id == id);
}

public sealed record InboxMessage(string Id, string Body, string Batch);

/// <summary>
/// A kind of source the framework does not ship, written the way a separate adapter package
/// would: it supplies only what is its own — listing the inbox, the item for a message (its body,
/// and its batch in the context metadata), and acknowledging a handled message. Everything else is
/// the framework's <see cref="SourceSweep{TEntry, TItem}"/>.
/// </summary>
public sealed record InboxSourcePort(string Name, Inbox Inbox) : SourcePort(Name)
{
    public const string BatchKey = "inbox_batch";

    public override ISourceSweep CreateSweep(SourceSweepContext context) => new InboxSweep(this, context);

    private sealed class InboxSweep(InboxSourcePort source, SourceSweepContext context)
        : SourceSweep<InboxMessage, InboxItem>(context, source.Name)
    {
        protected override IAsyncEnumerable<InboxMessage> ListAsync(CancellationToken ct) => source.Inbox.ListAsync(ct);

        protected override string IdOf(InboxMessage entry) =>
            source.Inbox.IdOf is { } idOf ? idOf(entry)! : entry.Id;

        protected override InboxItem CreateItem(InboxMessage entry, IServiceProvider scope) => new(entry, Port, scope);

        protected override Task CompleteAsync(InboxItem item, SweepOutcome outcome)
        {
            source.Inbox.Acknowledge(item.Id);
            return Task.CompletedTask;
        }

        protected override SweepItemTelemetry TelemetryOf(InboxMessage entry) =>
            source.Inbox.TelemetryOf is { } telemetryOf
                ? telemetryOf(entry)
                : new() { SpanTags = new Dictionary<string, object?> { ["inbox.batch"] = entry.Batch } };
    }

    private sealed class InboxItem(InboxMessage message, string port, IServiceProvider services)
        : SweptItem(message.Id, port, services)
    {
        public override IReadOnlyDictionary<string, string> Metadata { get; } =
            new Dictionary<string, string> { [BatchKey] = message.Batch };

        public override Task<byte[]> ReadAsync(CancellationToken ct = default) =>
            Task.FromResult(Encoding.UTF8.GetBytes(message.Body));
    }
}
