using Crisp_CSV_ETL;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Crisp_CSV_ETL_Runner
{
    public class Program
    {
        private const string _mappingsFilePath = "mappings.json";
        public static async Task Main(string[] args)
        {
            // Clean sample file            
            var prefix = "orders-sample";
            Console.WriteLine($"Begin transforming {prefix}.csv");
            using (var streamReader = new StreamReader($"{prefix}.csv"))
            using (var streamWriter = new StreamWriter($"{prefix}-transformed.csv"))
            using (var transformer = new CsvTransformer(_mappingsFilePath, $"{prefix}-errors.log")) 
            {
                await transformer.ReadCsvAsync(streamReader);
                await transformer.WriteTransformedCsvAsync(streamWriter);
            }
            Console.WriteLine($"End transforming {prefix}.csv");
            Console.WriteLine();

            // Sample file with errors
            prefix = "orders-invalid-sample";
            Console.WriteLine($"Begin transforming {prefix}.csv");
            using (var streamReader = new StreamReader($"{prefix}.csv"))
            using (var streamWriter = new StreamWriter($"{prefix}-transformed.csv"))
            using (var transformer = new CsvTransformer(_mappingsFilePath, $"{prefix}-errors.log"))
            {
                await transformer.ReadCsvAsync(streamReader);
                await transformer.WriteTransformedCsvAsync(streamWriter);
            }
            Console.WriteLine($"End transforming {prefix}.csv");
            Console.WriteLine();

            // Bigger sample file
            prefix = "orders-bigger-sample";
            Console.WriteLine($"Begin transforming {prefix}.csv");
            using (var streamReader = new StreamReader($"{prefix}.csv"))
            using (var streamWriter = new StreamWriter($"{prefix}-transformed.csv"))
            using (var transformer = new CsvTransformer(_mappingsFilePath, $"{prefix}-errors.log"))
            {
                const int batchSize = 500;
                var addHeader = true;
                do
                {
                    transformer.ImportRows.Clear();
                    await transformer.ReadCsvAsync(streamReader, batchSize);
                    await transformer.WriteTransformedCsvAsync(streamWriter, addHeader);
                    addHeader = false;
                    transformer.ResultRows.Clear();
                } while (transformer.ImportRows.Count > 0);
            }
            Console.WriteLine($"End transforming {prefix}.csv");
            Console.WriteLine();
        }
    }
}
