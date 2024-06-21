This is a data project I worked on using Formula1 Racing data in Databricks and Spark.

I proceeded as follows

1. Created a resource group in Azure to use a Databricks workspace, Azure Data Lake storage, app registration, and a key vault.
2. Used ADLS to create blob containers to house my different layers (raw, processed, presentation)
3. Took my app information (Client ID, Object ID, and Tenant ID) and turned them into secrets using key vault. Once the secrets were created, I linked my DBFS to my secret scope so that they can be easily accessible.
4. Once my secrets were managed, I created my ADLS mounts and attached them to my containers, creating mount points to send data to.
5. Now, I began the ingestion of raw files. I took csv,json, and csv/json folders and uploaded them into my raw container using Azure Storage Explorer.
6. I also had to make sure that my raw data can be accessible as external tables in the Hive MetaStore, so that I can use Spark-SQL against my raw data. (raw, processed, and presentation database and table creation)
7. Once my data was securely placed in my raw container, I wrote PySpark dataframes to read from those containers and perform transformations (such as removing columns, renaming columns, adding columns).
8. After all my transformations were completed, I wrote my data to the Hive MetaStore as managed tables in parquet format. This data was written to the processed database in HMS, which also wrote them to my ADLS containers.
9. Once my data was in the processed layer, I used Spark-SQL to perform joins, aggregations, and ranks to organize my data better. I combined many different data files to get a full picture of racing in Formula1.
10. After organizing my data, I wrote my datasets to the presentation layer.
11. I then used Spark-SQL to pull my data from HMS and analyze it using aggregations, ranks, and visualizations.
