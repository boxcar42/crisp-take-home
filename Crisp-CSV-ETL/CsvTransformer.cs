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
        public List<ResultRow> ResultRows { get; private set; } = new List<ResultRow>();
        private StreamWriter _logWriter = null;
        private int _rowIndex = 0;
        private CsvReader _csvReader = null;

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

        public async Task ReadCsvAsync(StreamReader streamReader, int batchSize = int.MaxValue)
        {
            if (streamReader == null)
                throw new ArgumentNullException(nameof(streamReader));

            if (_csvReader == null)
                _csvReader = new CsvReader(streamReader, ",");
            // New import session
            if (_rowIndex == 0)
            {
                _csvReader.Read();
                Headers = new HeaderRow(_csvReader);
                _rowIndex++;
            }
            while (ImportRows.Count < batchSize && _csvReader.Read())
            {
                var importRow = new ImportRow(Headers, _csvReader, _rowIndex++);
                ImportRows.Add(importRow);
            }

            await TransformCsvAsync();

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

        private async Task TransformCsvAsync()
        {
            var validImportRows = ImportRows.Where(r => r.IsValid);
            foreach (var importRow in validImportRows)
            {
                var row = new ResultRow(Mappings);
                row.TransformCsvRow(importRow);
                ResultRows.Add(row);
            }
        }

        public async Task WriteTransformedCsvAsync(StreamWriter streamWriter, bool addHeader = true)
        {
            if (streamWriter == null)
                throw new ArgumentNullException(nameof(streamWriter));

            var csvWriter = new CsvWriter(streamWriter);
            if (addHeader)
            {
                var emptyRow = new ResultRow(Mappings);
                foreach (var column in emptyRow.Columns)
                {
                    csvWriter.WriteField(column.Name);
                }
                csvWriter.NextRecord();
            }

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
    }
}
