# File Adapters

> File system access through Dapr bindings for SFTP, SMB file shares, local files, and Azure Blob Storage.

**Namespace:** `Intropy.Framework.Adapters.File`
**Assembly:** `Intropy.Framework.Adapters`

## IFileAdapter

Interface for all file operations. All implementations use Dapr bindings under the hood.

```csharp
public interface IFileAdapter
{
    Task<List<FileEntry>> ListAsync(CancellationToken ct = default);
    Task<byte[]> GetContentAsync(string fileName, CancellationToken ct = default);
    Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null, CancellationToken ct = default);
    Task DeleteAsync(string fileName, CancellationToken ct = default);
}
```

Text reads and writes with an encoding are `FileAdapterExtensions` over the binary core:

```csharp
Task<string> GetContentAsync(this IFileAdapter adapter, string fileName, Encoding encoding, CancellationToken ct = default);
Task WriteAsync(this IFileAdapter adapter, string fileName, string content, Encoding encoding, string? basePathOverride = null, CancellationToken ct = default);
```

### Methods

| Method | Description |
|--------|-------------|
| `ListAsync(ct)` | Lists files matching the configured filter. Returns `List<FileEntry>` whose names are relative to `BasePath`. |
| `GetContentAsync(fileName, ct)` | Reads file content as `byte[]`. Throws on a missing file. |
| `GetContentAsync(fileName, encoding, ct)` | Extension: reads file content as `string` with the specified encoding. |
| `WriteAsync(fileName, content, basePathOverride, ct)` | Writes `byte[]` content. Optional `basePathOverride` to write to a different directory. |
| `WriteAsync(fileName, content, encoding, basePathOverride, ct)` | Extension: writes `string` content with encoding. |
| `DeleteAsync(fileName, ct)` | Deletes a file. |

---

## FileAdapterOptions

Configuration for file adapters.

```csharp
public class FileAdapterOptions
{
    public string DaprBindingName { get; }
    public string BasePath { get; }
    public Regex? FileNameRegex { get; }
}
```

| Property | Type | Description |
|----------|------|-------------|
| `DaprBindingName` | `string` | Name of the Dapr binding component |
| `BasePath` | `string` | Base directory path for file operations |
| `FileNameRegex` | `Regex?` | Optional regex to filter files in `ListAsync()` |

---

## FileEntry

**Namespace:** `Intropy.Framework.Adapters.Common`

```csharp
public record FileEntry(string Name, long? Size = null, DateTimeOffset? ModifiedAt = null);
```

Returned by `IFileAdapter.ListAsync()`. Contains the file name relative to the configured `BasePath`,
and the file's size and last modification time when the binding's listing reports them (the SMB
binding does; the SFTP, local and blob listings leave them `null`).

---

## Implementations

### SftpAdapter

Connects to SFTP servers via a Dapr SFTP binding component.

```csharp
var adapter = new SftpAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "sftp-binding",
    basePath: "/incoming/orders",
    fileNameRegex: new Regex(@"\.csv$")));
```

Filters the listing response to exclude directories (`isDirectory: false`).

### SmbAdapter

Connects to Windows file shares via the Intropy SMB binding (`bindings.smb`), a pluggable
component that follows the SFTP binding's operations and metadata. Select it with `Kind: Smb`.

```csharp
var adapter = new SmbAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "partner-share",
    basePath: "xmlout"));
```

On top of the SFTP shape it uses what SMB can guarantee:

- **Exclusive reads.** `GetContentAsync` asks the binding for an exclusive open. A file someone
  else still has open is refused instead of read half written; a sweep counts it as failed and
  handles it on the next run.
- **Idempotent retried writes.** The binding never overwrites. When a write is refused and the file
  is there with exactly the same content, an earlier attempt landed, and the write succeeds
  (the span is tagged `intropy.file.already_written`). Different content is still an error.
- **Moves.** `MoveAsync(fileName, destinationBasePath, newFileName)` uses the binding's atomic
  `rename`, without reading or rewriting the content. It never overwrites.
  `FileCompletion.Archive` uses it on an SMB source.
- **Size and modification time** in listings.

### LocalFileAdapter

Reads from local or mounted file systems via a Dapr local storage binding.

```csharp
var adapter = new LocalFileAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "local-storage",
    basePath: "/mnt/data/orders"));
```

### AzureBlobStorageAdapter

Reads from Azure Blob Storage via a Dapr Azure Blob Storage binding.

```csharp
var adapter = new AzureBlobStorageAdapter(daprClient, new FileAdapterOptions(
    daprBindingName: "blob-storage",
    basePath: "orders/incoming"));
```

Uses the `blobName` metadata key for blob operations, and sends the base path as the listing
`prefix` in the request body rather than in metadata.

Azure has no directories, so a blob's prefix is part of its name and the binding's `list` returns
full blob names. The adapter strips the configured prefix, keeping listings relative like the other
adapters'. A nested blob keeps its relative path: with `basePath: "orders/incoming"`, the blob
`orders/incoming/2026/order.json` lists as `2026/order.json`, and `GetContentAsync` on that name
resolves back to the full blob. A blob returned outside the configured prefix is listed unchanged
rather than dropped.

---

## Tracing

Every operation of the built-in adapters is a span on the `Intropy.Framework.Adapters` source,
named `{operation} {binding}` (for example `get orders-source`). The operation is the Dapr binding
operation: `list`, `get`, `create` or `delete`, and `rename` for an SMB move.

| Attribute | On | Value |
|-----------|----|-------|
| `intropy.file.adapter` | all | `local`, `sftp`, `smb` or `azure_blob` |
| `intropy.file.binding` | all | The Dapr binding name |
| `intropy.file.operation` | all | `list`, `get`, `create`, `delete` or `rename` |
| `file.directory` | `list` | The base path listed |
| `intropy.file.listed` / `intropy.file.matched` | `list` | Files the binding returned / files left after `FileNameRegex` |
| `file.name`, `file.path` | `get`, `create`, `delete`, `rename` | The file name, and the path sent to the binding |
| `intropy.file.new_path` | `rename` | The path the file was moved to |
| `intropy.file.already_written` | `create` (SMB) | `true` when a refused write found the same content already there |
| `file.size` | `get`, `create` | Bytes read or written |
| `error.type` | failures | The exception type; the span status is `Error` |

A cancellation the caller requested leaves the span unmarked. When a file you expected isn't
picked up, compare `intropy.file.listed` with `intropy.file.matched` on the `list` span.

---

## Usage with Transactional Integration

File adapters are commonly used in receive pipelines to list and read source files:

```csharp
// Register the adapter
builder.Services.AddKeyedSingleton<IFileAdapter>("order-source", (sp, _) =>
    new SftpAdapter(
        sp.GetRequiredService<DaprClient>(),
        new FileAdapterOptions("sftp-binding", "/incoming", new Regex(@"\.xml$"))));

// Sweep it as a transactional integration's source: the lifecycle's FileSweep lists and reads it,
// and deletes or archives each file after the receive pipeline published it.
builder.Services.AddSourcePort("order-source"); // the adapter above is registered under the port name
builder.Services.AddTransactionalIntegration(opts =>
{
    // The internal queue defaults to the pub/sub and topic the deployment names in
    // INTROPY_PUBSUB_NAME and INTROPY_TOPIC_NAME.
    opts.IdleTimeout = TimeSpan.FromSeconds(5);
});
```

## See also

- [Transactional Integration](../blocks/transactional-integration.md) — receive pipeline architecture
- [Builders](../core/builders.md) — pipeline builder configuration
