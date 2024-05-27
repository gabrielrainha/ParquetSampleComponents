namespace ParquetComponents.BigFile
{
    public interface IBigFileService
    {
        Task<IBigFileWriter<TData>> CreateBigFileWriterAsync<TData>(string fileName, IBigFileSchema<TData> schema);
    }
}
