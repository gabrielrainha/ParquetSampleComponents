namespace ParquetComponents.Extensions
{
    public static class TypeExtensions
    {
        public static bool IsNullableType(this Type type) =>
            type.IsGenericType(typeof(Nullable<>));

        public static bool IsGenericType(this Type type, Type genericType) =>
            type.IsGenericType ?
                type.GetGenericTypeDefinition() == genericType :
                false;
    }
}
