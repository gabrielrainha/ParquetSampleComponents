namespace ParquetComponents.BigFile.Parquet
{
    public sealed class ParqueFileWriter<TData> : IBigFileWriter<TData>
    {
        private readonly ParquetBlobWriterWrapper _blobWapper;
        private readonly ParquetWriterWrapper _parquerWapper;
        private readonly ParquetSchemaWrapper<TData> _schemaWrapper;
        private readonly List<TData> _pendingData;

        private const int BatchSize = Constants.ParquetFileRowGroupSize;

        private int _committedRows;

        public ParqueFileWriter(ParquetWriterWrapper parquerWapper,
            ParquetBlobWriterWrapper blobWapper,
            ParquetSchemaWrapper<TData> schemaWrapper)
        {
            _blobWapper = blobWapper;
            _schemaWrapper = schemaWrapper;
            _parquerWapper = parquerWapper;
            _pendingData = new List<TData>();
        }

        public async Task WriteAsync(IEnumerable<TData> data)
        {
            _pendingData.AddRange(data);
            await TryWriteBatchToParquetAsync(BatchMode.OnlyFullBatches);
        }

        public async Task<BigFileWriterResult> CommitAsync()
        {
            await TryWriteBatchToParquetAsync(BatchMode.IncludeRest);
            await _blobWapper.WriteAsync(_parquerWapper.Stream);
            var result = new BigFileWriterResult { CommitedRows = _committedRows };

            _pendingData.Clear();
            _committedRows = 0;

            return result;
        }

        public void Dispose()
        {
            _parquerWapper.Dispose();
        }

        private async Task TryWriteBatchToParquetAsync(BatchMode mode)
        {
            var batches = mode == BatchMode.IncludeRest ?
                (int)Math.Ceiling(_pendingData.Count / (decimal)BatchSize) :
                _pendingData.Count / BatchSize;

            var tasks = Enumerable.Range(0, batches - 1)
                .Select(async i =>
                {
                    var batchData = _pendingData.Skip(i * BatchSize).Take(BatchSize).ToArray();
                    await WriteParquetColumnsAsync(batchData);
                    return batchData;
                });
            //Todo: parquet team says that the writer is not thread-safe, so this should be throughly tested
            var batchesWriten = await Task.WhenAll(tasks);

            var writenData = Flatten(batchesWriten);
            foreach (var item in writenData)
                _pendingData.Remove(item);

            _committedRows += writenData.Count();
        }

        private async Task WriteParquetColumnsAsync(IEnumerable<TData> data)
        {
            using var groupWriter = _parquerWapper.ParquetWriter.CreateRowGroup();
            //parquet team says that is important that the columns are writen in order inside a rowGroup, so no paralelization here
            foreach (var column in _schemaWrapper.GetParquetColumns(data))
                await groupWriter.WriteColumnAsync(column);
        }

        private static IEnumerable<T> Flatten<T>(T[][] matrix) =>
            matrix.SelectMany(x => x);

        private enum BatchMode
        {
            OnlyFullBatches,
            IncludeRest
        }
    }
}
