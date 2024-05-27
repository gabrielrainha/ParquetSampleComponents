using System.Collections.Generic;

namespace ParquetComponents.BigFile
{
    public interface IBigFileSchema<TClass>
    {
        IEnumerable<BigFileField<TClass>> GetFields();
    }
}
