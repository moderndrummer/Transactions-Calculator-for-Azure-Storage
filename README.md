# Transactions-Calculator-for-Azure-Storage
a transactions calculator using the metrics table storage for a blob container (to compare costs V1 vs V2)

motivation: https://docs.microsoft.com/en-us/azure/storage/common/storage-account-upgrade?tabs=azure-cli

Calculating total vs READ/WRITE/DELETE etc transaction counts can be painful.
I needed a quick solution to calculate using the Azure metrics to effectively check if an upgrade to Azure Storage V2 does not incur much higher costs.

This is a quick and dirty solution to the problem.
