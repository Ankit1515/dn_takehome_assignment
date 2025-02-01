import os
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType
from src.config import PEOPLE_FILE_PATTERN, AUTOMOBILES_FILE_PATTERN, TICKETS_FILE_PATTERN

def get_files_by_pattern(directory: Path, pattern: str) -> list:
    """Getting all files matching the pattern in the directory"""
    return list(directory.glob(pattern))

def create_spark_session():
    return SparkSession.builder \
        .appName("TTPD Analysis") \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0") \
        .getOrCreate()

def read_people_data(spark: SparkSession, data_dir: Path) -> DataFrame:
    """Read and union all CSV files in the directory"""
    csv_files: list = get_files_by_pattern(data_dir, PEOPLE_FILE_PATTERN)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    print(f"Found {len(csv_files)} people data files:")
    
    # Reading first file to get cloumn names and then apply to all 
    result_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", "|") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(str(csv_files[0]))
    
    # Cleaning column names by removing quotes
    for old_col in result_df.columns:
        new_col = old_col.replace('"', '')
        result_df = result_df.withColumnRenamed(old_col, new_col)
    
    # print("\nDebug - CSV Schema:")
    # result_df.printSchema()
    # print("\nDebug - Sample data:")
    # result_df.show(2, truncate=False)
    
    # Union with remaining files
    for file in csv_files[1:]:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", "|") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("multiLine", "true") \
            .csv(str(file))
        
        # Clean column names by removing quotes
        for old_col in df.columns:
            new_col = old_col.replace('"', '')
            df = df.withColumnRenamed(old_col, new_col)
            
        result_df = result_df.union(df)
    
    return result_df

def read_automobiles_data(spark: SparkSession, data_dir: Path) -> DataFrame:
    """Read and union all XML files in the directory"""
    xml_files = get_files_by_pattern(data_dir, AUTOMOBILES_FILE_PATTERN)
    if not xml_files:
        raise FileNotFoundError(f"No XML files found in {data_dir}")
    
    print(f"Found {len(xml_files)} automobile data files:")
    
    # Read first file
    result_df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", "automobile") \
        .load(str(xml_files[0]))
    
    # Union with remaining files
    for file in xml_files[1:]:
        df = spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", "automobile") \
            .load(str(file))
        result_df = result_df.union(df)
    
    return result_df

def read_tickets_data(spark: SparkSession, data_dir: Path) -> DataFrame:
    """Read and union all JSON files in the directory"""
    json_files = get_files_by_pattern(data_dir, TICKETS_FILE_PATTERN)
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {data_dir}")
    
    print(f"Found {len(json_files)} ticket data files:")
    
    # Reading first file
    # seprating the array of tickets into row
    # making cloumns from tickets row items
    result_df = spark.read.json(str(json_files[0])) \
        .select(explode("speeding_tickets").alias("ticket")) \
        .select("ticket.*")
    
    # Union with remaining files
    for file in json_files[1:]:
        df = spark.read.json(str(file)) \
            .select(explode("speeding_tickets").alias("ticket")) \
            .select("ticket.*")
        result_df = result_df.union(df)
    
    return result_df 