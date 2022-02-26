using Crisp_CSV_ETL.Models;
using Newtonsoft.Json.Linq;
using NReco.Csv;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Crisp-CSV-ETL.Tests")]
namespace Crisp_CSV_ETL
{
    public class CsvTransformer : IDisposable
    {
        protected internal JToken Mappings { get; }
        protected HeaderRow Headers { get; set; }
        public List<ImportRow> ImportRows { get; private set; } = new List<ImportRow>();
        public List<Row> ResultRows { get; private set; } = new List<Row>();
        private StreamWriter _logWriter = null;

        public CsvTransformer(string mappingsFilePath) : this(mappingsFilePath, null)
        {
        }
        public CsvTransformer(string mappingsFilePath, string errorLogFilePath)
        {
            if (errorLogFilePath != null)
                _logWriter = new StreamWriter(errorLogFilePath);
            Mappings = LoadMappings(mappingsFilePath);
        }

        private JToken LoadMappings(string filePath)
        {
            JObject obj = JObject.Parse(File.ReadAllText(filePath));
            return obj["columns"];
        }

        public async Task ReadCsvAsync(StreamReader streamReader)
        {
            var csvReader = new CsvReader(streamReader, ",");
            csvReader.Read();
            Headers = new HeaderRow(csvReader);
            var rowIndex = 1;
            while (csvReader.Read())
            {
                var importRow = new ImportRow(Headers, csvReader, rowIndex++);
                ImportRows.Add(importRow);
            }

            await ImportCsvAsync();

            if (_logWriter != null)
            {
                // Log validation errors
                foreach (var invalidRow in ImportRows.Where(r => !r.IsValid))
                {
                    foreach (var validationError in invalidRow.ValidationErrors)
                    {
                        await _logWriter.WriteLineAsync($"[ValidationError] {validationError}");
                    }
                }

                // Log transform errors
                foreach (var invalidRow in ResultRows.Where(r => !r.IsValid))
                {
                    foreach (var transformError in invalidRow.TransformErrors)
                    {
                        await _logWriter.WriteLineAsync($"[TransformError] {transformError}");
                    }
                }
            }
        }

        private async Task ImportCsvAsync()
        {
            var validImportRows = ImportRows.Where(r => r.IsValid);
            foreach (var importRow in validImportRows)
            {
                var row = new Row(Mappings);
                row.TransformCsvRow(importRow);
                ResultRows.Add(row);
            }
        }

        public async Task WriteTransformedCsvAsync(StreamWriter streamWriter)
        {
            var csvWriter = new CsvWriter(streamWriter);
            var emptyRow = new Row(Mappings);
            foreach(var column in emptyRow.Columns)
            {
                csvWriter.WriteField(column.Name);
            }
            csvWriter.NextRecord();

            foreach (var row in ResultRows.Where(r => r.IsValid))
            {
                foreach (var column in row.Columns)
                {
                    csvWriter.WriteField(column.Value.ToString());
                }
                csvWriter.NextRecord();
            }
        }

        public void Dispose()
        {
            if (_logWriter != null)
            {
                _logWriter.Flush();
                _logWriter.Close();
                _logWriter.Dispose();
            }
        }

        // todo: write tests



        // todo: documentation
    }
}
