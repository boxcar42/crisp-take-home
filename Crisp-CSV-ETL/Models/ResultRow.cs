using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Crisp_CSV_ETL.Models
{
    public class ResultRow
    {
        public List<ResultColumn> Columns { get; set; } = new List<ResultColumn>();
        public List<string> TransformErrors { get; } = new List<string>();
        public bool IsValid { get => !TransformErrors.Any(); }
        public ResultRow() { }
        public ResultRow(JToken columnMappings)
        {
            foreach (var mapping in columnMappings)
            {
                ResultColumn column;
                switch (mapping["type"].ToString())
                {
                    case "Integer":
                        column = new ResultColumn<int>();
                        break;
                    case "Date":
                        column = new ResultColumn<DateTime>();
                        break;
                    case "BigDecimal":
                        column = new ResultColumn<decimal>();
                        break;
                    case "String":
                    default:
                        column = new ResultColumn<string>();
                        break;
                }
                column.Name = mapping["name"].ToString();
                if (mapping["format"] != null)
                {
                    column.Format = Enum.TryParse(typeof(ColumnFormatType), mapping["format"].ToString(), out var format) ? (ColumnFormatType)format : ColumnFormatType.None;
                }
                column.MapFrom = mapping["mapFrom"].ToString();
                this.Columns.Add(column);
            }
        }

        public void TransformCsvRow(ImportRow importRow)
        {
            foreach (var column in Columns)
            {
                var transformedValue = "";
                try
                {
                    transformedValue = TransformValue(importRow, column.MapFrom);
                    column.ParseValue(transformedValue);
                }
                catch
                {
                    TransformErrors.Add($"Row {importRow.RowIndex}, header \"{column.Name}\" - Bad transformed value: \"{transformedValue}\"");
                }
            }
        }

        private string TransformValue(ImportRow importRow, string mapFrom)
        {
            var rg = new Regex(@"(\{.+?\})");
            var evaluator = new MatchEvaluator((match) =>
            {
                var headerName = match.Value.Substring(1, match.Value.Length - 2);
                return importRow[headerName];
            });
            var value = rg.Replace(mapFrom, evaluator);
            return value;
        }
    }
}
