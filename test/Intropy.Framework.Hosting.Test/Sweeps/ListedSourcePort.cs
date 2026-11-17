using System.Text;
using Intropy.Framework.Hosting.Sweeps;

namespace Intropy.Framework.Hosting.Test.Sweeps;

/// <summary>
/// A source port of a kind other than files, for tests: it hands out a fixed list of items, each
/// in its own scope and with its own metadata, and records which items were completed with which
/// outcome. It stands in for the source kinds to come (a polled query, an HTTP poll).
/// </summary>
internal sealed record ListedSourcePort(string Name, IReadOnlyList<ListedItem> Items) : SourcePort(Name)
{
    /// <summary>The items the sweep completed, in completion order.</summary>
    public List<(string Id, SweepOutcome Outcome)> Completed { get; } = [];

    public override ISourceSweep CreateSweep(SourceSweepContext context) => new Sweep(this, context);

    private sealed class Sweep(ListedSourcePort source, SourceSweepContext context)
        : SourceSweep<ListedItem, Item>(context, source.Name)
    {
        protected override IAsyncEnumerable<ListedItem> ListAsync(CancellationToken ct) =>
            source.Items.ToAsyncEnumerable();

        protected override string IdOf(ListedItem entry) => entry.Id;

        protected override Item CreateItem(ListedItem entry, IServiceProvider scope) => new(entry, Port, scope);

        protected override Task CompleteAsync(Item item, SweepOutcome outcome)
        {
            source.Completed.Add((item.Id, outcome));
            return Task.CompletedTask;
        }
    }

    private sealed class Item(ListedItem listed, string port, IServiceProvider services)
        : SweptItem(listed.Id, port, services)
    {
        public override IReadOnlyDictionary<string, string> Metadata => listed.Metadata;

        public override Task<byte[]> ReadAsync(CancellationToken ct = default) =>
            Task.FromResult(Encoding.UTF8.GetBytes(listed.Content));
    }
}

/// <summary>One item a <see cref="ListedSourcePort"/> hands out.</summary>
internal sealed record ListedItem(string Id, string Content, IReadOnlyDictionary<string, string> Metadata);
