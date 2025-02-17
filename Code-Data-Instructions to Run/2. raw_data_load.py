import pandas as pd
import psycopg2

DB_CONFIG = {
    "dbname": "WHO_Disease_Tracker",
    "user": "postgres",
    "password": "<Insert Your Password>",
    "host": "localhost",
    "port": "5432",
}

df = pd.read_csv('location_data.csv')
patient_df = pd.read_csv('patient_data.csv')
medicine_df = pd.read_csv('medicine.csv')
medicine_df.insert(0, 'ID', range(1,140)) # only first time, never again
disease_df = pd.read_csv('disease.csv')
disease_type_df = pd.read_csv('disease_type.csv')
medicine_disease_interaction_df = pd.read_csv('medicine_disease_interaction_table.csv')
race_disease_propensity_df = pd.read_csv('Race_Disease_table.csv')
patient_disease_df = pd.read_csv('diseased_patient_data_set.csv')

def insert_location_to_postgres(df):
    try:
        country_data = df[["Country Id", "Country", "Developing Country Flag", "Wealth Index Score"]].drop_duplicates()
        country_data.columns = ["country_id", "country_name", "developing_flag", "wealth_rank_number"]

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Inserting Values into Country_Index Table")
        for _, row in country_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.country_index (Country_ID, Country_Name, Wealth_Rank_Number, Developing_Flag)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (Country_ID) DO NOTHING;
                """,
                (row["country_id"], row["country_name"], row["wealth_rank_number"], row["developing_flag"]),
            )
            #print(f'inserted {_+1} record(s)')
        print("Data inserted successfully!")
        location_data = df[["Location Id", "City", "State", "Country"]]
        location_data.columns = ["city_id", "city_name", "state_province_name", "country_name"]
        print("Inserting Values into Location Table")
        for _, row in location_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.location (City_ID, City_Name, State_Province_Name, Country_Name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (City_ID) DO NOTHING;
                """,
                (row["city_id"], row["city_name"], row["state_province_name"], row["country_name"]),
            )
            #print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        # cur.close()     
        conn.close()

def insert_race_details_to_postgres():

    race_details = [('WHT', 'White'),
                	('BLK', 'Black or African American'),
                	('ASN', 'Asian'),
                	('HIS', 'Hispanic or Latino'),
                	('IND', 'Native American or Alaska Native'),
                	('PAI', 'Pacific Islander'),
                	('MID', 'Middle Eastern or North African'),
                	('NHW', 'Native Hawaiian'),
                	('MIR', 'Mixed Race'),
                	('OTH', 'Other')
                   ]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("Inserting Values into Race Table")
    for race_code, race_desc in race_details:
        cur.execute(f"INSERT INTO raw_data_staging_layer.race (Race_Code, Race_Description) VALUES ('{race_code}','{race_desc}') ON CONFLICT (Race_Code) DO NOTHING;")
    conn.commit()
    print("Data inserted successfully!")
        

def insert_patients_to_postgres(patient_df):
    try:
        patient_df['race_cd'] = patient_df['race_cd'].astype(str).str.zfill(2)

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        patient_data = patient_df[['person_id', 'last_name', 'first_name', 'gender', 'race_cd', 'primary_location_id']]
        patient_data.columns = ['person_id', 'last_name', 'first_name', 'gender', 'race_code', 'location_id']
        print("Inserting Values into Person Table")
        for _, row in patient_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.person (person_id, last_name, first_name, gender, race_code, location_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (person_id) DO NOTHING;
                """,
                (row["person_id"], row["last_name"], row["first_name"], row["gender"], row["race_code"], row["location_id"]),
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()

def insert_medicines_to_postgres(medicine_df):
    try:
        
        # medicine_df.fillna('Null')

        medicine_data = medicine_df[["ID", "Medicine Name", "Standard Industry Number (SIN)", "Company", "Active Ingredient Name"]]

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Inserting Values into Medicine Table")
        for _,row in medicine_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.Medicine (Medicine_ID, Medicine_Name, Standard_Industry_Number, Company, Active_Ingredient_Name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Medicine_ID) DO NOTHING;
                """,
                (row["ID"], row["Medicine Name"], row["Standard Industry Number (SIN)"], row["Company"], row["Active Ingredient Name"]),
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()


def insert_disease_type_to_postgres(disease_type_df):
    try:
        # disease_type_df.fillna('Null')
        disease_type_data = disease_type_df[["ID", "Disease Type", "Description"]]

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("Inserting Values into Disease_Type Table")
        for _,row in disease_type_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.Disease_Type(Disease_Type_Code, Disease_Type_Name, Disease_Type_Description)
                VALUES (%s, %s, %s)
                ON CONFLICT (Disease_Type_Code) DO NOTHING;
                """,
                (row["ID"],row["Disease Type"], row[ "Description"]),
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()

def insert_disease_to_postgres(disease_df):
    try:
        disease_data = disease_df[["diseaseid", "diseasename", "diseasetype", "sourcediseasecd", "intensitylevel"]]
        disease_data["sourcediseasecd"] = disease_data["sourcediseasecd"].fillna(0)
        disease_data["sourcediseasecd"] = disease_data["sourcediseasecd"].astype(int)
        

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Inserting Values into Disease Table")
        for _,row in disease_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.Disease (Disease_ID, Disease_Name, Intensity_Level_Qty, Disease_Type_Cd, Source_Disease_Cd)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Disease_ID) DO NOTHING;
                """,
                (row["diseaseid"], row["diseasename"], row["intensitylevel"], row["diseasetype"], row["sourcediseasecd"]),
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()


def insert_medicine_disease_interaction_to_postgres(medicine_disease_interaction_df):
    try:
        medicine_disease_interaction_data = medicine_disease_interaction_df[['row_ID', 'Disease_Code', 'Disease_Name', 'Medicine_ID', 'First_Available_Date', 'Effectiveness_Percent']]
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Inserting Values into medicine_disease_interaction Table")
        for _,row in medicine_disease_interaction_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.medicine_disease_interaction (row_ID, Disease_Code, Disease_Name, Medicine_ID, First_Available_Date, Effectiveness_Percent)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (row_ID) DO NOTHING;
                """,
                (row['row_ID'], row['Disease_Code'], row['Disease_Name'], row['Medicine_ID'], row['First_Available_Date'], row['Effectiveness_Percent'])
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()


def insert_race_disease_propensity_to_postgres(race_disease_propensity_df):
    try:
        race_disease_propensity_data = race_disease_propensity_df[['Race_Code', 'Race_Description', 'Disease_ID', 'Disease_Name','Propensity_Value']]
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Inserting Values into Race Disease propensity Table")
        for _,row in race_disease_propensity_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.Race_Disease_Propensity (Race_Code, Race_Description, Disease_ID, Disease_Name,Propensity_Value)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (row['Race_Code'], row['Race_Description'], row['Disease_ID'], row['Disease_Name'], row['Propensity_Value'])
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()

def insert_diseases_patient_to_postgres(diseased_patient_df):
    try:
        diseased_patient_data = patient_disease_df[['person_id', 'disease_code', 'disease_name', 'severity', 'start_date','end_date']]
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("Inserting Values into Diseased Patient Table")
        for _,row in diseased_patient_data.iterrows():
            cur.execute(
                """
                INSERT INTO raw_data_staging_layer.Diseased_Patient (Person_ID, Disease_Code, Disease_Name, Severity, start_date, end_date)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (row['person_id'], row['disease_code'], row['disease_name'], row['severity'], row['start_date'], row['end_date']),
            )
            # print(f'inserted {_+1} record(s)')

        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error:", e)
    finally:
        cur.close()     
        conn.close()

if __name__ == "__main__":
    insert_location_to_postgres(df)
    insert_race_details_to_postgres()
    insert_patients_to_postgres(patient_df)
    insert_medicines_to_postgres(medicine_df)
    insert_disease_type_to_postgres(disease_type_df)
    insert_disease_to_postgres(disease_df)
    insert_medicine_disease_interaction_to_postgres(medicine_disease_interaction_df)
    insert_race_disease_propensity_to_postgres(race_disease_propensity_df)
    insert_diseases_patient_to_postgres(patient_disease_df)
    
