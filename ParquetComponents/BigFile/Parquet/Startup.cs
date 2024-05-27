using Microsoft.Extensions.DependencyInjection;

namespace ParquetComponents.BigFile.Parquet
{
    public static class Startup
    {
        //Todo: could add some explicit configuration here instead of relying on IOptions
        public static IServiceCollection AddParquetBigFileService(this IServiceCollection services)
        {
            return services.AddTransient<IBigFileService, ParquetFileService>();
        }
    }
}
