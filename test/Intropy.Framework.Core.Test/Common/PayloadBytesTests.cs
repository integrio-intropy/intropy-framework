using Intropy.Framework.Core.Common;

namespace Intropy.Framework.Core.Test.Common;

public class PayloadBytesTests
{
    [Fact]
    public void AsArray_ReturnsTheBackingArray_WhenTheMemorySpansAllOfIt()
    {
        var array = new byte[] { 1, 2, 3 };

        Assert.Same(array, PayloadBytes.AsArray(array));
    }

    [Fact]
    public void AsArray_CopiesASlice()
    {
        var array = new byte[] { 0, 1, 2, 3, 0 };

        var result = PayloadBytes.AsArray(array.AsMemory(1, 3));

        Assert.Equal(new byte[] { 1, 2, 3 }, result);
        Assert.NotSame(array, result);
    }

    [Fact]
    public void AsArray_ReturnsAnEmptyArray_ForEmptyMemory()
    {
        Assert.Empty(PayloadBytes.AsArray(ReadOnlyMemory<byte>.Empty));
    }
}
