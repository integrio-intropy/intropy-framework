# File Adapters

> File system access through Dapr bindings for SFTP, local files, and Azure Blob Storage.

**Namespace:** `Intropy.Framework.Adapters.File`
**Assembly:** `Intropy.Framework.Adapters`

## IFileAdapter

Interface for file operations. The built-in SFTP, local, and Azure Blob adapters in this source revision use Dapr bindings. These signatures come from [IFileAdapter.cs](../../src/Intropy.Framework.Adapters/File/IFileAdapter.cs); use the documentation revision matching your installed package.

```csharp
using System.Text;
using FileInfo = Intropy.Framework.Adapters.Common.FileInfo;

public interface IFileAdapter
{
    Task<List<FileInfo>> ListAsync();
    Task<byte[]> GetContentAsync(string fileName);
    Task<string?> GetContentAsync(string fileName, Encoding encoding);
    Task WriteAsync(string fileName, byte[] content, string? basePathOverride = null);
    Task WriteAsync(string fileName, string content, Encoding encoding, string? basePathOverride = null);
    Task DeleteAsync(string fileName);
}
```

### Methods

| Method | Description |
|--------|-------------|
| `ListAsync()` | Lists files matching the configured filter. Returns `List<FileInfo>`. |
| `GetContentAsync(fileName)` | Reads file content as `byte[]`. |
| `GetContentAsync(fileName, encoding)` | Reads file content as `string` with the specified encoding. |
| `WriteAsync(fileName, content)` | Writes `byte[]` content. Optional `basePathOverride` to write to a different directory. |
| `WriteAsync(fileName, content, encoding)` | Writes `string` content with encoding. |
| `DeleteAsync(fileName)` | Deletes a file. |

### Choosing a write overload

- **Text (`string`):** pass an explicit `Encoding`, such as `Encoding.UTF8` for a UTF-8 destination. There is no `WriteAsync(fileName, stringContent)` overload or implicit default text encoding.
- **Binary or already-encoded content (`byte[]`):** use the byte overload. The adapter does not decode or re-encode it.
- In both overloads, `basePathOverride` is an optional destination-directory override. It is the **third** argument for bytes and the **fourth** for text; it is not an encoding name.

```csharp
using System.Text;
using Intropy.Framework.Adapters.File;

static async Task WriteExamplesAsync(IFileAdapter adapter)
{
    const string text = "{\"status\":\"ready\"}";
    await adapter.WriteAsync("result.json", text, Encoding.UTF8);
    await adapter.WriteAsync("result.json", text, Encoding.UTF8, basePathOverride: "/outgoing");

    byte[] bytes = Encoding.UTF8.GetBytes(text);
    await adapter.WriteAsync("result.json", bytes, basePathOverride: "/outgoing");
}
```

The built-in text overload calls `encoding.GetBytes(content)` and delegates to the byte overload. Choosing the text overload is a convenience, not a requirement for all outbound writes. Use the destination's required encoding; `Encoding.UTF8` here is an example, not a framework-wide policy. `GetBytes` does not prepend an encoding preamble/BOM; provide those bytes yourself if the destination requires one.

None of the `IFileAdapter` methods accepts a cancellation token. A step can check its token before calling an adapter, but cannot pass it through this interface to cancel an in-flight operation.

The byte-read signature is `Task<byte[]>`; the text-read signature is `Task<string?>`. Do not infer a portable “missing file returns null” policy from that nullable annotation: the built-in adapters propagate binding exceptions. Handle missing-file behavior according to the configured binding.

---

## FileAdapterOptions

Configuration for file adapters. Constructor:

```csharp
public FileAdapterOptions(string daprBindingName, string basePath = "", Regex? fileNameRegex = null);
```

Read-only properties (`Regex` is `System.Text.RegularExpressions.Regex`):

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

## FileInfo

**Namespace:** `Intropy.Framework.Adapters.Common`

```csharp
public record FileInfo(string FileName);
```

Returned by `IFileAdapter.ListAsync()`. Contains the file name.

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

### LocalFileAdapter

Reads from local or mounted file systems (e.g., SMB shares) via a Dapr local storage binding.

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

Uses the `blobName` metadata key for blob operations and handles path prefix normalization.

---

## Usage with Transactional Integration

Register an `IFileAdapter` in your application's DI container, then inject it into the source lister and receive step:

```csharp
using Intropy.Framework.Adapters.File;
using Intropy.Framework.Blocks.Shared;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive;
using Intropy.Framework.Blocks.TransactionalIntegration.Receive.Steps;
using Intropy.Framework.Core.Pipeline.Abstractions.Results;

// Use in source lister
public class FileSourceLister(IFileAdapter fileAdapter) : ISourceLister
{
    public async Task<IReadOnlyList<SourceItemInfo>> ListItemsAsync(
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var files = await fileAdapter.ListAsync();
        return files.Select(f => new SourceItemInfo(f.FileName)).ToList();
    }
}

// Use in receive step
public class FileReceiver(IFileAdapter fileAdapter) : ReceiveStep<Context>
{
    public override async Task<(BusinessStepResult<SourceItem> Result, Context Context)> ExecuteAsync(
        SourceItemInfo input, Context context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var content = await fileAdapter.GetContentAsync(input.Id);
        return (new BusinessStepResult<SourceItem>.Success(new SourceItem(input.Id, content)), context);
    }
}
```

## See also

- [Implementing pipeline steps](../implementing-pipeline-steps.md) — a text sender with the correct result family
- [IFileAdapter.cs](../../src/Intropy.Framework.Adapters/File/IFileAdapter.cs) — interface source
- [LocalFileAdapter.cs](../../src/Intropy.Framework.Adapters/File/LocalFileAdapter.cs) — text-to-byte conversion and binding calls
- [Transactional Integration](../blocks/transactional-integration.md) — receive pipeline architecture
- [Builders](../core/builders.md) — pipeline builder configuration
