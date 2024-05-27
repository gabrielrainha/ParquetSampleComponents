using System;
using System.Linq.Expressions;

namespace ParquetComponents.BigFile
{
    public class BigFileField<TClass>
    {
        private readonly Func<TClass, object> _valueFunc;

        public BigFileField(string propertyName, 
            Type propertyType, 
            Func<TClass, object> valueFunc)
        {
            _valueFunc = valueFunc;

            PropertyName = propertyName;
            PropertyType = propertyType;
        }

        public string PropertyName { get; }
        public Type PropertyType { get; set; }

        public object GetPropertyValue(TClass instance) =>
            _valueFunc?.Invoke(instance);
    }

    //Todo: Need to test performance because of the type-cast necessary for extracting the property name. In case it's too slow, use the base version instead
    public class BigFileField<TClass, TField> : BigFileField<TClass>
    {
        public BigFileField(Expression<Func<TClass, TField>> valueExpression)
            : base(GetFuncPropertyName(valueExpression), typeof(TField), GetValueFunc(valueExpression))
        { }

        private static string GetFuncPropertyName(Expression<Func<TClass, TField>> expr)
        {
            if (expr.Body is MemberExpression memberExpression)
                return memberExpression.Member.Name;

            throw new ArgumentException($"The provided expression contains a {expr.GetType().Name} which is not supported. " +
                $"Only simple member accessors (fields, properties) of an object are supported.");
        }

        private static Func<TClass, object> GetValueFunc(Expression<Func<TClass, TField>> valueExpression)
        {
            var compiled = valueExpression.Compile();
            return instance => compiled(instance);
        }
    }
}
