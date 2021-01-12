using System;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;

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
            var totalWriteV2 = filtered.Where(GetWritePredicate()).Sum(i => i.TotalBillableRequests);
            var totalDeleteV2 = filtered.Where(GetDeletePredicate()).Sum(i => i.TotalBillableRequests);
            var totalReadV2 = totalV1 - totalDeleteV2 - totalWriteV2;

            Console.WriteLine($"Total Write Transactions (V2): {totalWriteV2}");
            Console.WriteLine($"Total Delete Transactions (V2): {totalDeleteV2}");
            Console.WriteLine($"Total Read Transactions (V2): {totalReadV2}");
            Console.WriteLine();
            Console.WriteLine($"Total Transactions (V1): {totalV1}");
        }

        private static Func<HourPrimaryTransactionsBlobRow, bool> GetWritePredicate() => PredicatesWithOr<HourPrimaryTransactionsBlobRow>("put", "append", "list", "create", "snapshot", "copy");
        private static Func<HourPrimaryTransactionsBlobRow, bool> GetDeletePredicate() => PredicatesWithOr<HourPrimaryTransactionsBlobRow>("delete");
        private static CloudTable GetTable()
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable tableRef = tableClient.GetTableReference(tablename);
            return tableRef;
        }

        public static IQueryable<HourPrimaryTransactionsBlobRow> All(int daysAgo) => GetTable().CreateQuery<HourPrimaryTransactionsBlobRow>()
            .Where(i => i.Timestamp > DateTime.UtcNow.AddDays(-daysAgo) && i.TotalBillableRequests > 0);

        private static Func<T, bool> PredicatesWithOr<T>(params string[] keys) where T : TableEntity
        {
            var predicate = PredicateBuilder.False<T>();
            foreach (var key in keys)
            {
                predicate = predicate.Or(i => i.RowKey.Contains(key, StringComparison.InvariantCultureIgnoreCase));
            }
            return predicate;
        }
    }

    public class HourPrimaryTransactionsBlobRow : TableEntity
    {
        public long TotalBillableRequests { get; set; }
        public long TotalEgress { get; set; }
        public long TotalIngress { get; set; }

        public HourPrimaryTransactionsBlobRow() { }
    }

    public static class PredicateBuilder
    {
        public static Func<T, bool> False<T>() { return f => false; }
        public static Func<T, bool> Or<T>(this Func<T, bool> expr1, Func<T, bool> expr2) => t => expr1(t) || expr2(t);
    }

}
