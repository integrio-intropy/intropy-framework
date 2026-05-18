namespace Intropy.Framework.Core.Pipeline.Abstractions;

/// <summary>
/// Interface used by idempotency checks to generate a hash from an object
/// </summary>
public interface IHashable
{
    /// <summary>
    /// Method that generates a hash that represents the object
    /// </summary>
    /// <returns></returns>
    string GetHashString();
}