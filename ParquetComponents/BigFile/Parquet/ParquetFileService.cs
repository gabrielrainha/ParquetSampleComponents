using Microsoft.Extensions.Options;
using Parquet;
using ParquetComponents.Storage;
using System.IO.Compression;

namespace ParquetComponents.BigFile.Parquet
{
    public class ParquetFileService : IBigFileService
    {
        private readonly IBlobStorage _blobStorage;
        private readonly string _containerName;

        public ParquetFileService(IBlobStorage blobStorage, IOptions<ClientConfig> options)
        {
            _blobStorage = blobStorage;
            _containerName = options.Value.StorageConnectionString;
        }

        public async Task<IBigFileWriter<TData>> CreateBigFileWriterAsync<TData>(string fileName, IBigFileSchema<TData> schema)
        {
            var memoryStream = new MemoryStream();
            var schemaWrapper = new ParquetSchemaWrapper<TData>(schema);
            var parquetWriter = await ParquetWriter.CreateAsync(schemaWrapper.GetParquetSchema(), memoryStream);
            parquetWriter.CompressionMethod = CompressionMethod.Gzip;
            parquetWriter.CompressionLevel = CompressionLevel.Optimal;

            return new ParqueFileWriter<TData>(
                new ParquetWriterWrapper(memoryStream, parquetWriter),
                new ParquetBlobWriterWrapper(_blobStorage, _containerName, fileName),
                schemaWrapper
            );
        }
    }
}
