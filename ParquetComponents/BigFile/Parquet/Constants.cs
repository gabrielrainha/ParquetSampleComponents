namespace ParquetComponents.BigFile.Parquet
{
    //Those are some placeholders for the actual constants used on RPAM.API
    public class Constants
    {
        //This one originally resides on EY.RPAM.Domain.Constants.PartnerAllocationConstant
        public const int ParquetFileRowGroupSize = 100000;
        //And this one on EY.RPAM.Application.Common.ApplicationConstant. It's called RPAM_SA_CONTAINER there.
        public const string CONTAINER = "rpam";
    }
}
