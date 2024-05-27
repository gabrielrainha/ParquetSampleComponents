using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetComponents.Extensions;
using ParquetComponents.Storage;
using System.Text;

namespace ParquetComponents.BigFile.Parquet
{
    public sealed class ParquetWriterWrapper : IDisposable
    {
        public ParquetWriterWrapper(Stream stream, ParquetWriter parquetWriter)
        {
            ParquetWriter = parquetWriter;
            Stream = stream;
        }

        public Stream Stream { get; }
        public ParquetWriter ParquetWriter { get; }

        public void Dispose()
        {
            Stream?.Dispose();
            ParquetWriter?.Dispose();
        }
    }

    public sealed class ParquetBlobWriterWrapper
    {
        private readonly IBlobStorage _blobStorage;
        private readonly string _container;
        private readonly string _filePath;

        public ParquetBlobWriterWrapper(IBlobStorage blobStorage, string containerName, string filePath)
        {
            _blobStorage = blobStorage;
            _container = containerName;
            _filePath = filePath;
        }

        public async Task WriteAsync(Stream stream)
        {
            stream.Position = 0;
            var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));
            await _blobStorage.StageBlockBlobAsync(Constants.CONTAINER, _container, _filePath, stream, blockId);
            await _blobStorage.CommitBlockBlobAsync(Constants.CONTAINER, _container, _filePath, new[] { blockId });
        }

        public async Task<Stream> ReadAsync()
        {
            return await _blobStorage.DownloadBlob(Constants.CONTAINER, _container, _filePath);
        }
    }

    public sealed class ParquetSchemaWrapper<TData>
    {
        private readonly IEnumerable<SchemaWrapperData> _fields;

        public ParquetSchemaWrapper(IBigFileSchema<TData> schemaFields)
        {
            _fields = GetSchemaFields(schemaFields);
        }

        public ParquetSchema GetParquetSchema() =>
            new(_fields.Select(p => p.ParquetField).ToArray());

        public IEnumerable<DataColumn> GetParquetColumns(IEnumerable<TData> data)
        {
            foreach (var fieldWrapper in _fields)
            {
                var dataValues = data.Select(fieldWrapper.Field.GetPropertyValue);
                yield return new DataColumn(
                    fieldWrapper.ParquetField,
                    GetParquetDataArray(dataValues.ToArray(), fieldWrapper.ParquetField)
                );
            }
        }

        private static Array GetParquetDataArray(object[] data, DataField field)
        {
            static Array GetNullableSafeData<T>(DataField field, object[] data)
                where T : struct
            {
                if (field.IsNullable)
                    return data.Cast<T?>().ToArray();
                else
                    return data.Cast<T>().ToArray();
            }

            return Type.GetTypeCode(field.ClrType) switch
            {
                TypeCode.Int32 => GetNullableSafeData<int>(field, data),
                TypeCode.Int64 => GetNullableSafeData<long>(field, data),
                TypeCode.Double => GetNullableSafeData<double>(field, data),
                TypeCode.Decimal => GetNullableSafeData<decimal>(field, data),
                TypeCode.Boolean => GetNullableSafeData<bool>(field, data),
                TypeCode.DateTime => GetNullableSafeData<DateTimeOffset>(field, data),
                _ => field.ClrType == typeof(Guid) ?
                    GetNullableSafeData<Guid>(field, data) :
                    data.Select(value => value?.ToString()).ToArray(),
            };
        }

        private static IEnumerable<SchemaWrapperData> GetSchemaFields(IBigFileSchema<TData> schema) =>
            schema.GetFields()
                .Select(p =>
                {
                    var parquetField = new DataField(p.PropertyName, p.PropertyType, p.PropertyType.IsNullableType());
                    return new SchemaWrapperData(parquetField, p);
                })
                .ToArray();


        private sealed class SchemaWrapperData
        {
            public SchemaWrapperData(DataField parquetField, BigFileField<TData> field)
            {
                ParquetField = parquetField;
                Field = field;
            }

            public DataField ParquetField { get; }
            public BigFileField<TData> Field { get; }
        }
    }
}
