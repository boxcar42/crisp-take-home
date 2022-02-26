using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Crisp_CSV_ETL.Models
{
    public enum ColumnFormatType
    {
        None = 0,
        ProperCased = 1
    }

    public abstract class ResultColumn
    {
        public string Name { get; set; }
        public string MapFrom { get; set; }
        public ColumnFormatType Format { get; set; } = ColumnFormatType.None;
        public abstract Type Type { get; }
        public abstract object Value { get; set; }
        public abstract void ParseValue(string value);
    }

    public class ResultColumn<T> : ResultColumn
    {
        private T _value;
        public override object Value
        {
            get
            {
                return _value;
            }
            set
            {
                _value = (T)value;
            }
        }
        public override Type Type { get { return typeof(T); } }

        public override void ParseValue(string value)
        {
            if (typeof(T) == typeof(string))
            {
                if (Format == ColumnFormatType.ProperCased)
                {
                    TextInfo textInfo = CultureInfo.CurrentCulture.TextInfo;
                    value = textInfo.ToTitleCase(value);
                }
            }
            _value = (T)Convert.ChangeType(value, typeof(T));
        }
    }
}
