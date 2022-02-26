using Crisp_CSV_ETL.Models;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Crisp_CSV_ETL.Tests
{
    public class UnitTests
    {
        string mappingsFilePath = "mappings.json";

        [Fact]
        public void Create_Row_With_Configured_Mappings()
        {
            var parser = new CsvTransformer(mappingsFilePath);
            var row = new Row(parser.Mappings);

            Assert.Equal(6, row.Columns.Count);
            Assert.Equal(1, row.Columns.Count(x => x.Format == Models.ColumnFormatType.ProperCased));
            Assert.Equal(1, row.Columns.Count(x => x.Type == typeof(int)));
            Assert.Equal(1, row.Columns.Count(x => x.Type == typeof(DateTime)));
            Assert.Equal(1, row.Columns.Count(x => x.Type == typeof(decimal)));
            Assert.Equal(3, row.Columns.Count(x => x.Type == typeof(string)));
        }

        [Fact]
        public void Column_Requires_Proper_Type()
        {
            var column = new Column<int>();

            Assert.Throws<InvalidCastException>(() => column.Value = "bad value");
        }

        [Fact]
        public async Task Validate_Values_In_CSV_Finds_Errors()
        {
            var transformer = new CsvTransformer(mappingsFilePath);
            using (var streamReader = new StreamReader("orders-invalid-sample.csv"))
            {
                await transformer.ReadCsvAsync(streamReader);
            }
            Assert.Equal(5, transformer.ImportRows.Sum(x => x.ValidationErrors.Count));
        }

        [Fact]
        public async Task CSV_Values_Are_Transformed()
        {
            var transformer = new CsvTransformer(mappingsFilePath);
            using (var streamReader = new StreamReader("orders-sample.csv"))
            {
                await transformer.ReadCsvAsync(streamReader);                
            }
            Assert.Equal(2, transformer.ResultRows.Count);
        }
    }
}
