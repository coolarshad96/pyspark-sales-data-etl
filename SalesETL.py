from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_replace,sum, month, year, to_date, format_number
import os
import shutil

# Initialize SparkSession
spark = SparkSession.builder.appName('SalesETL').getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  ##optional

# Load CSV data into a DataFrame
sales_df=spark.read.csv('./data/Sales-Export_2019-2020.csv',header=True,inferSchema=True)

# Show a sample of the data
sales_df.show(5)
sales_df.printSchema()

rows=sales_df.count()
cols=len(sales_df.columns)
print(f"Shape: ({rows},{cols})")

# Remove spaces from column names
for col_name in sales_df.columns:
    sales_df=sales_df.withColumnRenamed(col_name,col_name.strip())

# Remove commas and convert to double type
sales_df = sales_df.withColumn("order_value_EUR", regexp_replace("order_value_EUR", ",", "").cast("double"))
sales_df = sales_df.withColumn("cost", regexp_replace("cost", ",", "").cast("double"))

sales_df.show(5)
sales_df.printSchema()

# Drop rows with any null values
cleaned_df = sales_df.dropna()

# Filter for valid sales records (e.g., non-negative quantities)
cleaned_df = cleaned_df.filter((col("cost") >= 0) & (col("order_value_EUR") >= 0))

# Parse the corrected date string to a DateType using default parser
cleaned_df = cleaned_df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

# Calculate total sales per region
country_sales_df = cleaned_df.groupBy("country").agg(sum("order_value_EUR").alias("total_sales"))
# Format the 'total_sales' column to 2 decimal places
country_sales_df = country_sales_df.withColumn(
    "total_sales_formatted", format_number("total_sales", 2)
)
# Show the results
country_sales_df.show(truncate=False)

# Calculate total sales per product category
category_sales_df = cleaned_df.groupBy("category").agg(sum("order_value_EUR").alias("total_category_wise_sales"))
# Format the 'total_sales' column to 2 decimal places
category_sales_df = category_sales_df.withColumn(
    "category_sales_formatted", format_number("total_category_wise_sales", 2)
)
category_sales_df.show(truncate=False)


# Calculate monthly sales trends
monthly_sales_df = cleaned_df \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .groupBy("year", "month") \
    .agg(sum("order_value_EUR").alias("monthly_sales"))
# Format the 'total_sales' column to 2 decimal places
monthly_sales_df = monthly_sales_df.withColumn(
    "monthly_sales_formatted", format_number("monthly_sales", 2)
)
monthly_sales_df.show(truncate=False)

##top selling sales_rep
top_salesman_df = cleaned_df.groupBy('sales_rep') \
    .agg(sum('order_value_EUR').alias("top_salesman")) \
    .sort(col("top_salesman").desc())

top_salesman_df = top_salesman_df.withColumn(
    "top_salesman_formatted", format_number("top_salesman", 2)
)
top_salesman_df.show()

##top selling manager
top_salesmanager_df = cleaned_df.groupBy('sales_manager') \
    .agg(sum('order_value_EUR').alias("top_salesmanager")) \
    .sort(col("top_salesmanager").desc())

top_salesmanager_df = top_salesmanager_df.withColumn(
    "top_salesmanager_formatted", format_number("top_salesmanager", 2)
)
top_salesmanager_df.show()

# Group by 'sales_manager' and 'sales_rep', calculate the sum of 'order_value_EUR'
top_sales_df = cleaned_df.groupBy('sales_manager', 'sales_rep') \
    .agg(sum('order_value_EUR').alias("total_sales")) \
    .sort(col("total_sales").desc())

# Format the 'total_sales' column to two decimal places
top_sales_df = top_sales_df.withColumn(
    "total_sales_formatted", format_number("total_sales", 2)
)

# Show the result
top_sales_df.show(truncate=False)


#Store data
output_path = "./data/"  # Update this with your desired output path

def check_and_remove(path):
    if os.path.exists(path):
        shutil.rmtree(path)  # Removes the directory and its contents
        print(f"Removed existing directory: {path}")
    else:
        print(f"Directory does not exist, no need to remove: {path}")

# Define specific paths for each DataFrame
country_sales_path = output_path + "country_sales"
category_sales_path = output_path + "category_sales"
monthly_sales_path = output_path + "monthly_sales"
top_salesman_path = output_path + "top_salesman"
top_salesmanager_path = output_path + "top_salesmanager"
top_sales_path = output_path + "top_sales"

# Check and remove directories before saving new files
check_and_remove(country_sales_path)
check_and_remove(category_sales_path)
check_and_remove(monthly_sales_path)
check_and_remove(top_salesman_path)
check_and_remove(top_salesmanager_path)
check_and_remove(top_sales_path)

# Save each DataFrame to CSV
country_sales_df.write.csv(country_sales_path)
category_sales_df.write.csv(category_sales_path)
monthly_sales_df.write.csv(monthly_sales_path)
top_salesman_df.write.csv(top_salesman_path)
top_salesmanager_df.write.csv(top_salesmanager_path)
top_sales_df.write.csv(top_sales_path)

# terminate seesion
spark.stop()