using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Queryable;


namespace AzureStorage
{
    public class Program
    {
        private const string tablename = "$MetricsHourPrimaryTransactionsBlob";

        private const string connectionString = "XXXXXXXXXXX"; //Primary Blob Connection String
        private static int daysAgo = 31;

        static void Main()
        {
            var items = All(daysAgo).ToList();

            // in memory filter relevant only (not supported by CosmosDB):
            var filtered = items.Where(i => i.RowKey.StartsWith("user;", StringComparison.InvariantCultureIgnoreCase)).ToList();

            var totalV1 = filtered.Sum(i => i.TotalBillableRequests);
            var totalWriteV2i = filtered.Where(GetWritePredicate()).ToList();
            var totalWriteV2 = filtered.Where(GetWritePredicate()).Sum(i => i.TotalBillableRequests);
            var totalDeleteV2 = filtered.Where(GetDeletePredicate()).Sum(i => i.TotalBillableRequests);
            var totalReadV2 = totalV1 - totalDeleteV2 - totalWriteV2;

            Console.WriteLine($"Total Write Transactions (V2): {totalWriteV2}");
            Console.WriteLine($"Total Delete Transactions (V2): {totalDeleteV2}");
            Console.WriteLine($"Total Read Transactions (V2): {totalReadV2}");
            Console.WriteLine();
            Console.WriteLine($"Total Transactions (V1): {totalV1}");
        }

        private static Func<HourPrimaryTransactionsBlobRow, bool> GetWritePredicate() =>
            i => i.RowKey.Contains("put", StringComparison.InvariantCultureIgnoreCase) ||
            i.RowKey.Contains("append", StringComparison.InvariantCultureIgnoreCase) ||
            i.RowKey.Contains("list", StringComparison.InvariantCultureIgnoreCase) ||
            i.RowKey.Contains("create", StringComparison.InvariantCultureIgnoreCase) ||
            i.RowKey.Contains("snapshot", StringComparison.InvariantCultureIgnoreCase) ||
            i.RowKey.Contains("copy", StringComparison.InvariantCultureIgnoreCase);

        private static Func<HourPrimaryTransactionsBlobRow, bool> GetDeletePredicate() => i => i.RowKey.Contains("delete", StringComparison.InvariantCultureIgnoreCase);

        private static CloudTable GetTable()
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable tableRef = tableClient.GetTableReference(tablename);
            return tableRef;
        }

        public static IQueryable<HourPrimaryTransactionsBlobRow> All(int daysAgo) => GetTable().CreateQuery<HourPrimaryTransactionsBlobRow>()
            .Where(i => i.Timestamp > DateTime.UtcNow.AddDays(-daysAgo) && i.TotalBillableRequests > 0);
    }

    public class HourPrimaryTransactionsBlobRow : TableEntity
    {
        public long TotalBillableRequests { get; set; }
        public long TotalEgress { get; set; }
        public long TotalIngress { get; set; }

        public HourPrimaryTransactionsBlobRow() { }
    }
}
