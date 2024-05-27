namespace ParquetComponents.Storage
{
    //That's an excerpt from RPAM.API, located on EY.RPAM.Application.Interfaces.IBlobStorage
    public interface IBlobStorage
    {
        Task<MemoryStream> DownloadBlob(string container, string blobId, string storageAccount);
        Task CommitBlockBlobAsync(string container, string blob, string storageAccount, IEnumerable<string> blockIds);
        Task StageBlockBlobAsync(string container, string blob, string storageAccount, Stream content, string blockId);

    }
}