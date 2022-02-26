using NReco.Csv;
using System;
using System.Collections.Generic;
using System.Text;

namespace Crisp_CSV_ETL.Models
{
    public class HeaderRow
    {
        private List<Header> _headers = new List<Header>();

        public int this[string header]
        {
            get
            {
                return _headers.FindIndex(x => x.Name == header);
            }
        }

        public string this[int index]
        {
            get
            {
                return _headers[index].Name;
            }
        }

        public HeaderRow(CsvReader csvReader)
        {
            _headers = new List<Header>();
            for (var i = 0; i < csvReader.FieldsCount; i++)
            {
                _headers.Add(new Header(csvReader[i]));
            }
        }

        private class Header
        {
            public string Name { get; }
            public Header(string name)
            {
                Name = name;
            }
        }
    }
}
