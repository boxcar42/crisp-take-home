using NReco.Csv;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Crisp_CSV_ETL.Models
{
    public class ImportRow
    {
        private HeaderRow _headers;
        private List<string> _columns;

        public int RowIndex { get; }
        public string this[string headerName]
        {
            get
            {
                return _columns[_headers[headerName]];
            }
        }
        public List<string> ValidationErrors { get; } = new List<string>();
        public bool IsValid { get => !ValidationErrors.Any(); }
        public ImportRow(HeaderRow headers, CsvReader csvReader, int rowIndex)
        {
            _headers = headers;
            _columns = new List<string>();
            RowIndex = rowIndex;
            ParseRow(csvReader);
        }
        public void ParseRow(CsvReader csvReader)
        {
            for (var i = 0; i < csvReader.FieldsCount; i++)
            {
                if (Validate(_headers[i], csvReader[i]))
                {
                    _columns.Add(csvReader[i]);
                }
                else
                {
                    ValidationErrors.Add($"Row {RowIndex}, header \"{_headers[i]}\" - Value: \"{csvReader[i]}\" is not valid.");
                    _columns.Add(null);
                }
            }
        }

        private bool Validate(string headerName, string value)
        {
            string rgPattern;
            switch (headerName)
            {
                case "Order Number":
                    rgPattern = @"^\d+$";
                    break;
                case "Year":
                    rgPattern = @"^\d{4}$";
                    break;
                case "Month":
                    rgPattern = @"^\d{1,2}$";
                    break;
                case "Day":
                    rgPattern = @"^\d{1,2}$"; 
                    break;
                case "Product Number":
                    rgPattern = @"^[A-Za-z0-9]+$";
                    break;
                case "Product Name":
                    rgPattern = @"^[A-Za-z ]+$";
                    break;
                case "Count":
                    rgPattern = @"^\d{1,3}(,?\d{3})*(\.\d+)?$";
                    break;
                default:
                    rgPattern = "";
                    break;
            }
            var rg = new Regex(rgPattern);
            var match = rg.Match(value);
            return match.Success;
        }

    }
}
