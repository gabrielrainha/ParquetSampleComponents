namespace ParquetComponents.BigFile
{
    public interface IBigFileWriter<TData> : IDisposable
    {
        Task WriteAsync(IEnumerable<TData> data);
        Task<BigFileWriterResult> CommitAsync();
    }
}
