YouTube Analytics Data Pipeline Documentation
This documentation outlines the steps and code snippets used to build a YouTube analytics data pipeline. The pipeline involves extracting data from an Azure Data Lake Storage Gen2 (ADLS Gen2) data source, transforming it using Azure Databricks and Apache Spark, and finally storing the processed data in ADLS Gen2 for further analysis. The data is then made available for reporting and visualization in Power BI.

Table of Contents
Bronze Layer
Silver Layer
Gold Layer
Final Data Export
1. Bronze Layer<a name="bronze-layer"></a>
1.1 Data Ingestion
python
Copy code
# Import required libraries
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Define the directory to check
directory_to_check = "/mnt/bronze"
mounted_directories = [mount.mountPoint for mount in dbutils.fs.mounts()]

# Check if the directory is already mounted, if not, mount it
if directory_to_check in mounted_directories:
    print(f"The directory {directory_to_check} is already mounted.")
else:
    dbutils.fs.mount(
        source="abfss://bronze@youtubeadlsg2.dfs.core.windows.net/",
        mount_point="/mnt/bronze",
        extra_configs=configs
    )

# Load data from Parquet files
file_path = '/mnt/bronze/raw_data.parquet'
df = spark.read.format('parquet').load(file_path)
1.2 Data Exploration
python
Copy code
# Display the first 5 rows of the DataFrame
df.show(5)

# Select specific columns for exploration
df.select("rank", "Youtuber", "subscribers", "video_views").show(5)
2. Silver Layer<a name="silver-layer"></a>
2.1 Data Cleaning and Transformation
python
Copy code
# Define numeric columns for conversion
numeric_columns = [
    "subscribers", "video_views", "video_views_rank", "country_rank",
    "channel_type_rank", "video_views_for_the_last_30_days",
    "lowest_monthly_earnings", "highest_monthly_earnings",
    "lowest_yearly_earnings", "highest_yearly_earnings",
    "subscribers_for_last_30_days", "Gross_tertiary_education_enrollment",
    "Population", "Unemployment_rate"
]

# Convert numeric columns to DoubleType
for column in numeric_columns:
    df = df.withColumn(column, col(column).cast(DoubleType()))

# Validate and convert date columns
date_columns = ["created_year", "created_month", "created_date"]
for column in date_columns:
    df = df.withColumn(column, col(column).cast(DateType()))

# Validate and convert categorical columns (e.g., to StringType)
categorical_columns = ["Youtuber", "category", "Title", "Country", "Abbreviation", "channel_type"]
for column in categorical_columns:
    df = df.withColumn(column, col(column).cast(StringType()))

# Remove duplicate rows based on specific columns
duplicate_columns = ['Youtuber', 'Country']
df_no_duplicates = df.dropDuplicates(subset=duplicate_columns)
2.2 Data Storage
python
Copy code
# Mount the Silver Layer directory
directory_to_check = "/mnt/silver"
mounted_directories = [mount.mountPoint for mount in dbutils.fs.mounts()]

if directory_to_check in mounted_directories:
    print(f"The directory {directory_to_check} is already mounted.")
else:
    dbutils.fs.mount(
        source="abfss://silver@youtubeadlsg2.dfs.core.windows.net/",
        mount_point="/mnt/silver",
        extra_configs=configs
    )

# Write the cleaned data to the Silver Layer in Delta format
output_path = '/mnt/silver/data'
df_no_duplicates.write.format('delta').mode('overwrite').save(output_path)
3. Gold Layer<a name="gold-layer"></a>
3.1 Feature Engineering
python
Copy code
# Calculate a new column "subscriber_to_view_ratio"
df = df.withColumn("subscriber_to_view_ratio", col("subscribers") / col("video_views"))

# Perform StringIndexing and One-Hot Encoding for the "Country" column
string_indexer = StringIndexer(inputCol="Country", outputCol="CountryIndex")
encoder = OneHotEncoder(inputCol="CountryIndex", outputCol="CountryOneHot")
pipeline = Pipeline(stages=[string_indexer, encoder])
model = pipeline.fit(df)
df_encoded = model.transform(df)

# Another example of StringIndexing for "Country"
indexer = StringIndexer(inputCol="Country", outputCol="CountryEncoded")
df_encoded = indexer.fit(df).transform(df)
3.2 Data Storage
python
Copy code
# Mount the Gold Layer directory
directory_to_check = "/mnt/gold"
mounted_directories = [mount.mountPoint for mount in dbutils.fs.mounts()]

if directory_to_check in mounted_directories:
    print(f"The directory {directory_to_check} is already mounted.")
else:
    dbutils.fs.mount(
        source="abfss://gold@youtubeadlsg2.dfs.core.windows.net/",
        mount_point="/mnt/gold",
        extra_configs=configs
    )

# Write the enriched data to the Gold Layer in Delta format
output_path = '/mnt/gold/data'
df_encoded.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(output_path)
4. Final Data Export<a name="final-data-export"></a>
4.1 Export to CSV
python
Copy code
# Mount the Final Data Export directory
directory_to_check = "/mnt/final"
mounted_directories = [mount.mountPoint for mount in dbutils.fs.mounts()]

if directory_to_check in mounted_directories:
    print(f"The directory {directory_to_check} is already mounted.")
else:
    dbutils.fs.mount(
        source="abfss://final@youtubeadlsg2.dfs.core.windows.net/",
        mount_point="/mnt/final",
        extra_configs=configs
    )

# Create a DeltaTable instance for the Gold Layer
delta_table = DeltaTable.forPath(spark, "/mnt/gold/data/")

# Read the Delta table into a DataFrame
delta_df = delta_table.toDF()

# Define the output path for the CSV file
csv_output_path = '/mnt/final/data/csv_data/'

# Write the DataFrame as a CSV file
delta_df.coalesce(1).write.format('csv').mode('overwrite').option('header', 'true').save(csv_output_path)
This pipeline demonstrates the process of ingesting, cleaning, transforming, and storing YouTube analytics data in multiple layers within an Azure Data Lake Storage Gen2 environment. The data is further enriched and made ready for analysis in Power BI.