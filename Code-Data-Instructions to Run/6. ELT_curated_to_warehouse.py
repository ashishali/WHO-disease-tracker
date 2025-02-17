import psycopg2

DB_CONFIG = {
    "dbname": "WHO_Disease_Tracker",
    "user": "postgres",
    "password": "<Insert Your Password>",
    "host": "localhost",
    "port": "5432",
}

def execute_query(conn, query, data=None):
    """Execute a single SQL query."""
    with conn.cursor() as cur:
        cur.execute(query, data)
        conn.commit()

def extract_data(conn):
    """Extract data from curated layer into temporary staging tables."""
    queries = [
        """
        CREATE TEMP TABLE staging_disease_type AS 
        SELECT * FROM curated_layer.Disease_type;
        """,
        """
        CREATE TEMP TABLE staging_disease AS 
        SELECT * FROM curated_layer.Disease;
        """,
        """
        CREATE TEMP TABLE staging_person AS 
        SELECT * FROM curated_layer.Person;
        """,
        """
        CREATE TEMP TABLE staging_diseased_patient AS 
        SELECT * FROM curated_layer.Diseased_Patient;
        """,
        """
        CREATE TEMP TABLE staging_race AS 
        SELECT * FROM curated_layer.Race;
        """,
        """
        CREATE TEMP TABLE staging_location AS 
        SELECT * FROM curated_layer.Location;
        """,
        """
        CREATE TEMP TABLE staging_medicine AS 
        SELECT * FROM curated_layer.Medicine;
        """,
        """
        CREATE TEMP TABLE staging_race_disease_propensity AS 
        SELECT * FROM curated_layer.race_disease_propensity;
        """,
        """
        CREATE TEMP TABLE staging_medicine_disease_interaction AS 
        SELECT * FROM curated_layer.medicine_disease_interaction;
        """
    ]
    for query in queries:
        execute_query(conn, query)
    print("Data extracted into staging tables.")

def transform_and_load(conn):
    """Transform and load data into warehouse tables.
       Most logic written here is toooooooo brain intensive, if it is working, then please don't touch it!!!!
    """
    # Load Disease_Type_Dimension
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Disease_Type_Dimension (disease_type_cd, disease_type_description)
        SELECT DISTINCT Disease_Type_Cd, Disease_Type_Description
        FROM staging_disease_type sdt, staging_disease sd where sdt.disease_type_code = sd.disease_type_cd;
    """)

    # Load Disease_Dimension
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Disease_Dimension (disease_id, disease_name, disease_type_cd, intensity_level)
        SELECT Disease_ID, Disease_Name, Disease_Type_Cd, Intensity_Level
        FROM staging_disease;
    """)

    # Load Location_Dimension
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Location_Dimension (location_id, city_name, state_province_name, country_name, developing_flag, wealth_rank_number, is_current_flag)
        SELECT Location_ID, City_Name, State_Province_Name, Country_Name, Developing_Flag, Wealth_Rank_Number,'Y'
        FROM staging_location;
    """)

     # Load Manufacturer_Dimension
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Manufacturer_Dimension 
			(manufacturer_id, manufacturer_name, is_current_flag)
        SELECT Distinct manufacturer_id, manufacturer_name, 'Y'
        FROM staging_medicine WHERE manufacturer_id IS NOT NULL;
    """)

    # Load Date_Dimension - I don't know what I did here, but it is working, so please don't touch this code!!!!!!!!!!!!!!!!!!!
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Date_Dimension (date_key, full_date, day, month, year, quarter, week, day_name, month_name)
        SELECT
            EXTRACT(YEAR FROM all_dates.full_date) * 10000 
            + EXTRACT(MONTH FROM all_dates.full_date) * 100 
            + EXTRACT(DAY FROM all_dates.full_date) AS date_key,
            all_dates.full_date,
            EXTRACT(DAY FROM all_dates.full_date) AS day,
            EXTRACT(MONTH FROM all_dates.full_date) AS month,
            EXTRACT(YEAR FROM all_dates.full_date) AS year,
            CASE
                WHEN EXTRACT(MONTH FROM all_dates.full_date) IN (1, 2, 3) THEN 1
                WHEN EXTRACT(MONTH FROM all_dates.full_date) IN (4, 5, 6) THEN 2
                WHEN EXTRACT(MONTH FROM all_dates.full_date) IN (7, 8, 9) THEN 3
                ELSE 4
            END AS quarter,
            EXTRACT(WEEK FROM all_dates.full_date) AS week,
            TO_CHAR(all_dates.full_date, 'Day') AS day_name,
            TO_CHAR(all_dates.full_date, 'Month') AS month_name
        FROM (
            SELECT DISTINCT DATE_TRUNC('month', dp.Start_Date) AS full_date
            FROM staging_diseased_patient dp
            UNION
            SELECT DISTINCT DATE_TRUNC('month', CURRENT_DATE) 
            UNION
            SELECT DISTINCT DATE_TRUNC('month', First_Available_Date) AS full_date 
            FROM staging_medicine_disease_interaction
            EXCEPT
            SELECT full_date FROM reporting_layer_DW.Date_Dimension
        ) all_dates;
    """)

    # Load Race_Gender_Dimension
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Race_Gender_Dimension (combination_id, race_code, race_description, gender)
        SELECT 
            ROW_NUMBER() OVER () AS combination_id
            ,Race_Code
            ,Race_Description
            ,Gender
            FROM(
                    SELECT 
                        DISTINCT Race_Code, Race_Description, Gender
                    FROM staging_person
                    JOIN staging_race ON staging_person.Race_CD = staging_race.Race_Code
                );
    """)

    # Load Diseased_Patient_Fact
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.Diseased_Patient_Fact (disease_id, location_id, combination_id, num_of_patients, race_propensity_value,
        avg_severity_value, start_of_month_date)
        SELECT
            dp.Disease_ID,
            ld.location_surrogate_key,
            rgd.combination_id,
            COUNT(dp.Patient_ID) AS num_of_patients,
            ROUND(AVG(st_race_disease_pro.Propensity_Value),0) AS Propensity_Value,
            round(AVG(dp.Severity_Value),0) AS avg_severity_value,
            DATE_TRUNC('month', dp.Start_Date) AS start_of_month_date
        FROM
            staging_diseased_patient dp
        JOIN staging_person p 
            ON dp.Patient_ID = p.Person_ID
        JOIN reporting_layer_DW.Race_Gender_Dimension rgd 
            ON p.Race_CD = rgd.race_code AND p.Gender = rgd.gender
        JOIN staging_race_disease_propensity AS st_race_disease_pro
            ON p.Race_CD = st_race_disease_pro.race_code AND dp.disease_id = st_race_disease_pro.disease_id
        JOIN reporting_layer_DW.Location_Dimension ld
            ON p.Primary_Location_ID = ld.location_id
        GROUP BY dp.Disease_ID, ld.location_surrogate_key, rgd.combination_id, DATE_TRUNC('month', dp.Start_Date)
    """)
    print("Data transformed and loaded into warehouse tables.")

    # Load Medicine_Fact
    execute_query(conn, """
        INSERT INTO reporting_layer_DW.medicine_fact (disease_id, manufacturer_id, First_Available_Date, num_of_medicines, avg_effectiveness_of_medicines)
        SELECT 
            disease_id,
            md.manufacturer_surrogate_key,
            MIN(DATE_TRUNC('month', i.First_Available_Date)) AS First_Available_Date,
            COUNT(m.medicine_id), 
            AVG(Effectiveness_Percent)
        FROM
            staging_medicine_disease_interaction as i
        JOIN staging_medicine as m
            ON i.medicine_id = m.medicine_id
        JOIN reporting_layer_DW.Manufacturer_Dimension as md
            ON  m.manufacturer_id = md.manufacturer_id
        GROUP BY
            disease_id, md.manufacturer_surrogate_key ;
                  """)

def drop_staging_tables(conn):
    """Drop temporary staging tables."""
    queries = [
        "DROP TABLE IF EXISTS staging_disease;",
        "DROP TABLE IF EXISTS staging_person;",
        "DROP TABLE IF EXISTS staging_diseased_patient;",
        "DROP TABLE IF EXISTS staging_race;",
        "DROP TABLE IF EXISTS staging_location;"
    ]
    for query in queries:
        execute_query(conn, query)
    print("Staging tables dropped.")

def main():
    """Main function to execute the ELT process."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        # Extract data into staging tables
        extract_data(conn)

        # load while transforming the data into the expected format to the warehouse tables
        transform_and_load(conn)

        # Drop staging tables
        drop_staging_tables(conn)

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
