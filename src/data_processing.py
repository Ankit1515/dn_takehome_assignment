from pyspark.sql.functions import col, year, month, concat, lit, when, lpad, lower
from src.config import TICKET_FEES

def identify_officers(people_df):
    """Identify officers from the people DataFrame based on profession and company"""
    
    return people_df.filter(
        lower(col('profession')).like("%police%") | 
        lower(col('company')).like("%police%") |
        lower(col('company')).like("%ttpd%")
    )

def calculate_ticket_fee(tickets_df):
    """Calculate ticket fees based on the rules"""
    
    return tickets_df.withColumn(
        "fee",
        when(
            col("school_zone_ind") & col("work_zone_ind"), 
            lit(TICKET_FEES["BOTH_ZONES"])
        ).when(
            col("school_zone_ind") | col("work_zone_ind"),
            lit(TICKET_FEES["SCHOOL_ZONE"])
        ).otherwise(
            lit(TICKET_FEES["BASE"])
        )
    )

def add_year_month(tickets_df):
    """Add year_month column for analysis"""
    return tickets_df.withColumn(
        "year_month",
        concat(
            year(col("ticket_time")),
            lit("-"),
            lpad(month(col("ticket_time")), 2, "0")
        )
    ) 