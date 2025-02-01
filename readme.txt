Delaware North: Take-Home Assignment

This project includes ‘main.py’, which is the initial scaffolding for your solution.
The `ttpd_data.zip’ file is your dataset.
This directory, once unzipped, will serve as your source file dataset that will need to be ingested and analyzed.
Make sure to utilize the Spark DataFrame API for the core of your solution; beyond that, you may use additional Python libraries as you see fit.
If you are using a Windows machine for this assignment, you may have some issues setting up Spark. If so, feel free to use this docker image to set up a local environment: https://hub.docker.com/r/jupyter/all-spark-notebook/.
Please save all code, results, and documentation to a git repository (e.g., GitHub, Bitbucket) that is available for us to access and review prior to our follow-up call to discuss your solution.
Feel free to reach out with any questions or issues if any arise.
The overall timeline for this assignment is 2-3 days, or 8 hours of work total.

1) Clone the repository locally.
2) Create a feature branch off main.
3) Submit a PR with comments about your update, along with any screenshots or outputs that would be helpful in the PR for testing.

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
