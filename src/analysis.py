from pyspark.sql.functions import count, sum, desc, concat, col

def get_top_ticket_issuing_officer(tickets_df, officers_df):
    return tickets_df.join(
        officers_df,
        tickets_df.officer_id == officers_df.id
    ).groupBy(
        "officer_id", "first_name", "last_name"
    ).agg(
        count("*").alias("ticket_count")
    ).orderBy(
        desc("ticket_count")
    ).limit(1)

def get_top_ticket_months(tickets_df):
    return tickets_df.groupBy(
        "year_month"
    ).agg(
        count("*").alias("ticket_count")
    ).orderBy(
        desc("ticket_count")
    ).limit(3)

def get_top_ticket_spenders(tickets_df, people_df, automobiles_df):
    return tickets_df.join(
        automobiles_df,
        tickets_df.license_plate == automobiles_df.license_plate
    ).join(
        people_df,
        automobiles_df.person_id == people_df.id
    ).groupBy(
        "person_id", "first_name", "last_name"
    ).agg(
        sum("fee").alias("total_fees")
    ).orderBy(
        desc("total_fees")
    ).limit(10) 