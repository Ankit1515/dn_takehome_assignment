"""
Delaware North: Take-Home Assignment

Technical Prerequisites:
  - Python 3+
  - Spark: pip install delta-spark
  - Java Runtime: https://java.com/en/download/manual.jsp

Assignment Background:
    - You are a freelance analytics consultant who has partnered with the TTPD (Tiny Town Police Department)
      to analyze speeding tickets that have been given to the adult citizens of Tiny Town over the 2020-2023 period.
    - Inside the folder "ttpd_data" you will find a directory of data for Tiny Town. This dataset will need to be "ingested" for analysis.
    - The solutions must use the Dataframes API.
    - You will need to ingest this data into a PySpark environment and answer the following three questions for the TTPD.

Questions:
    1. Which police officer was handed the most speeding tickets?
        - Police officers are recorded as citizens. Find in the data what differentiates an officer from a non-officer.
    2. What 3 months (year + month) had the most speeding tickets? 
        - Bonus: What overall month-by-month or year-by-year trends, if any, do you see?
    3. Using the ticket fee table below, who are the top 10 people who have spent the most money paying speeding tickets overall?

Ticket Fee Table:
    - Ticket (base): $30
    - Ticket (base + school zone): $60
    - Ticket (base + construction work zone): $60
    - Ticket (base + school zone + construction work zone): $120
"""

"""
Author: Ankit dubey
Date: 31-01-2025
"""

from src.config import *
from src.data_ingestion import create_spark_session, read_people_data, read_automobiles_data, read_tickets_data
from src.data_processing import identify_officers, calculate_ticket_fee, add_year_month
from src.analysis import get_top_ticket_issuing_officer, get_top_ticket_months, get_top_ticket_spenders

def main():
    # Setup data directory
    setup_data_directory()
    
    # taking spark session from data_ingestion.py
    spark = create_spark_session()
    
    try:
        # Data ingestion
        people_df = read_people_data(spark, DATA_DIR)
        automobiles_df = read_automobiles_data(spark, DATA_DIR)
        tickets_df = read_tickets_data(spark, DATA_DIR)
        
        # Printing record counts
        print(f"\nTotal records loaded:")
        print(f"People: {people_df.count():,}")
        print(f"Automobiles: {automobiles_df.count():,}")
        print(f"Tickets: {tickets_df.count():,}\n")
        
        print("\nSample Automobiles Data:") # automobiles data
        automobiles_df.show(5)
        
        print("\nSample People Data:") # people data
        people_df.show(5)
        
        # Data processing
        officers_df = identify_officers(people_df)
        print(f"\nIdentified {officers_df.count():,} officers")
        print("\nSample Officers Data:") # officers data
        officers_df.show(5)
        
        tickets_df = calculate_ticket_fee(tickets_df)
        tickets_df = add_year_month(tickets_df)
        print("\nSample Tickets Data:") # tickets data
        tickets_df.show(5)
        
        # Analysis
        print("\n1. Officer who issued the most tickets:")
        get_top_ticket_issuing_officer(tickets_df, officers_df).show()
        
        print("\n2. Top 3 months with most tickets:")
        get_top_ticket_months(tickets_df).show()
        
        print("\n3. Top 10 people who spent the most on tickets:")
        get_top_ticket_spenders(tickets_df, people_df, automobiles_df).show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
