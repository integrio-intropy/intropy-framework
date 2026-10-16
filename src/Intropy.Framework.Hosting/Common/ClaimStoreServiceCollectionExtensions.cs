using Intropy.Framework.Adapters.File;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Intropy.Framework.Hosting.Common;

/// <summary>
/// Registers the component's claim store: where a payload too large to ride inside a message envelope
/// is kept while a pointer to it travels instead (the claim check pattern).
/// </summary>
/// <remarks>
/// <para>
/// The store is deliberately not a port. Ports are the integration's edges — where business data
/// enters and leaves the system — and the topology models them directionally. A claim store is
/// neither: the payload goes in at one end of a queue hop and comes back out at the other, a single
/// Transactional Integration both writes and reads it, and no external system ever touches it. Its
/// analogue is the pub/sub component: transport plumbing, shared system-wide. Hence its own
/// registration and configuration section rather than an entry under <c>Ports</c>.
/// </para>
/// <para>
/// Registering a store is what makes the claim check possible; whether it engages is the component's
/// claim-check mode. Without a store every payload rides inside its message, exactly as before, and a
/// message that arrives carrying a pointer fails with a technical error naming the missing store.
/// </para>
/// </remarks>
public static class ClaimStoreServiceCollectionExtensions
{
    /// <summary>The configuration section holding the claim store's settings.</summary>
    public const string ClaimStoreSectionName = "ClaimStore";

    /// <summary>
    /// The key the claim store's <see cref="IFileAdapter"/> is registered under. Not a port name: the
    /// colon is why it cannot collide with a port the topology mints, which would otherwise trip the
    /// duplicate-key guard in <c>AddFileAdapter</c>.
    /// </summary>
    public const string ClaimStoreKey = "intropy:claim-store";

    /// <summary>
    /// Registers the claim store from the <c>ClaimStore</c> configuration section, which carries the
    /// same settings as a port (<c>Kind</c>, <c>DaprBindingName</c>, <c>BasePath</c>) and is validated
    /// the same way.
    /// </summary>
    /// <remarks>
    /// Only <see cref="IFileAdapter.WriteAsync"/> and <see cref="IFileAdapter.GetContentAsync"/> are
    /// ever used against it, and writes always go through the adapter's configured base path, never a
    /// per-write override — <see cref="IFileAdapter.GetContentAsync"/> has no equivalent override, so a
    /// payload written elsewhere could not be read back. The store is never listed (the adapters'
    /// listing semantics diverge, and a claim key is a path rather than a file name) and never deleted:
    /// expiring payloads is a storage lifecycle policy, owned by the platform.
    /// </remarks>
    /// <param name="services">The service collection to add services to.</param>
    /// <param name="configuration">The component's configuration.</param>
    public static IServiceCollection AddClaimStore(this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        return services.AddFileAdapter(ClaimStoreKey, configuration.GetSection(ClaimStoreSectionName));
    }
}
