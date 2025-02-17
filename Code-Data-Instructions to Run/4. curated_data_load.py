import psycopg2
from datetime import datetime
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

conn_params = {
        'dbname': 'WHO_Disease_Tracker',
        'user': 'postgres',
        'password': '<Insert Your Password>',
        'host': 'localhost',
        'port': 5432
    }

raw_data_layer_schema = "raw_data_staging_layer"
curated_data_schema = "curated_layer"

class disease_type_table():
    def __init__(self, conn_params):
        print("disease_type_table")
        self.raw_data_layer_table_name = "disease_type"
        self.curated_layer_table_name = "disease_type"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        transformed_data = raw_data
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Disease_Type_Code, Disease_Type_Name, Disease_Type_Description)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s);",
                                ( row["disease_type_code".lower()], row["disease_type_name".lower()], row["disease_type_description".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class race_table():
    def __init__(self, conn_params):
        print("race_table")
        self.raw_data_layer_table_name = "race"
        self.curated_layer_table_name = "race"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        transformed_data = raw_data
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Race_code, Race_Description)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s);",
                                ( row["Race_code".lower()], row["Race_Description".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class medicine_table():
    def __init__(self, conn_params):
        print("medicine_table")
        self.raw_data_layer_table_name = "medicine"
        self.curated_layer_table_name = "medicine"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        manufacturer_info = pd.read_csv("Manufacturer_Info.csv")
        transformed_data = pd.merge(raw_data, manufacturer_info, how = "left", left_on = "company", right_on = "Manufacturer_Name")
        transformed_data.drop(["company"], inplace= True, axis = 1)
        transformed_data.columns = [col.lower() for col in transformed_data.columns]
        transformed_data["manufacturer_id"] = transformed_data["manufacturer_id"].fillna(0)
        transformed_data["manufacturer_id"] = transformed_data["manufacturer_id"].astype(int)
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data["standard_industry_number"] = transformed_data["standard_industry_number"].replace("-",None)
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} \
                                    (Standard_Industry_Number, Name, Manufacturer_id, Manufacturer_Name, Active_Ingredient_Name)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s, %s, %s);",
                                ( row["Standard_Industry_Number".lower()], row["medicine_name".lower()],
                                     row["Manufacturer_id".lower()] if row["Manufacturer_id".lower()] > 0 else None,
                                    row["Manufacturer_Name".lower()], row["Active_Ingredient_Name".lower()])
                )

            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class location_table():
    def __init__(self, conn_params):
        print("location_table")
        self.raw_data_layer_table_name = "location"
        self.curated_layer_table_name = "location"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.country_index;"
            data = sqlio.read_sql_query(query, self.conn)
        except Exception as e:
            print("Error:", e)

        transformed_data = pd.merge(raw_data, data, how = "inner", on = "country_name")
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data["Wealth_Rank_Number".lower()] = transformed_data["Wealth_Rank_Number".lower()].apply(lambda x: 0 if x < 0 else 10 if x>10 else x )
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Location_ID, City_Name, State_Province_Name, Country_Name, \
                                Developing_Flag, Wealth_Rank_Number)"
            for _, row in cleaned_transformed_data.iterrows():
                try:
                    cur.execute(
                                    insert_into_stmt + " VALUES (%s, %s, %s, %s, %s, %s);",
                                    ( row["city_id".lower()], row["City_Name".lower()], row["State_Province_Name".lower()], row["Country_Name".lower()],
                                        row["Developing_Flag".lower()], row["Wealth_Rank_Number".lower()])
                    )
                except Exception as e:
                    print(row)
                    print("Error:", e)
                    
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class disease_table():
    def __init__(self, conn_params):
        print("disease_table")
        self.raw_data_layer_table_name = "disease"
        self.curated_layer_table_name = "disease"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        raw_data["disease_id"] = raw_data["disease_id"].astype(int)
        raw_data["source_disease_cd"] = raw_data["source_disease_cd"].astype(int)
        transformed_data = raw_data.sort_values(["source_disease_cd","disease_id"])
        
        try:
            query = f"SELECT * FROM {curated_data_schema}.disease_type;"
            data = sqlio.read_sql_query(query, self.conn)
        except Exception as e:
            print("Error:", e)

        transformed_data = pd.merge(transformed_data, data, how = "left", left_on = "disease_type_cd",right_on = "disease_type_name")
        transformed_data["disease_type_cd"] = transformed_data["disease_type_code"]
        
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data["source_disease_cd"] = transformed_data["source_disease_cd"].replace(0,None)
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Disease_ID, Disease_Name, Intensity_Level, \
                                    Disease_Type_Cd, Source_Disease_Cd)"
            for _, row in cleaned_transformed_data.iterrows():
                try:
                    cur.execute(
                                    insert_into_stmt + " VALUES (%s, %s, %s, %s, %s);",
                                    ( row["Disease_ID".lower()], row["Disease_Name".lower()], row["intensity_level_qty".lower()], row["Disease_Type_Cd".lower()],
                                      row["Source_Disease_Cd".lower()])
                    )
                   
                except Exception as e:
                    print(row)
                    print("Error:", e)
            self.conn.commit()
            
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class person_table():
    def __init__(self, conn_params):
        print("person_table")
        self.raw_data_layer_table_name = "person"
        self.curated_layer_table_name = "person"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        transformed_data = raw_data
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Person_ID, Last_Name, First_Name, Gender,\
                                    Primary_Location_ID, Race_CD)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s, %s, %s, %s);",
                                ( row["Person_ID".lower()], row["Last_Name".lower()], row["First_Name".lower()], row["Gender".lower()],
                                  row["Location_ID".lower()], row["Race_Code".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class patient_disease_table():
    def __init__(self, conn_params):
        print("patient_disease_table")
        self.raw_data_layer_table_name = "Diseased_Patient"
        self.curated_layer_table_name = "Diseased_Patient"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        try:
            query = f"SELECT * FROM {curated_data_schema}.disease;"
            data = sqlio.read_sql_query(query, self.conn)
        except Exception as e:
            print("Error:", e)

        transformed_data = pd.merge(raw_data, data, how = "inner", left_on = "disease_name",right_on = "disease_name")

        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Patient_ID, Disease_ID, Severity_Value, \
                                    Start_Date, End_Date)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s, %s, %s);",
                                ( row["Person_ID".lower()], row["Disease_ID".lower()], row["severity".lower()], row["Start_Date".lower()],
                                  row["End_Date".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class medicine_disease_interaction_table():
    def __init__(self, conn_params):
        print("medicine_disease_interaction_table")
        self.raw_data_layer_table_name = "medicine_disease_interaction"
        self.curated_layer_table_name = "medicine_disease_interaction"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
        print("\n------------------------------------------------------\n")
    
    def transform_raw_data(self,raw_data):
        try:
            query = f"SELECT * FROM {curated_data_schema}.disease;"
            data = sqlio.read_sql_query(query, self.conn)
        except Exception as e:
            print("Error:", e)

        transformed_data = pd.merge(raw_data, data, how = "inner", left_on = "disease_name",right_on = "disease_name")

        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Medicine_ID, Disease_ID, First_Available_Date, \
                                    Effectiveness_Percent)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s, %s);",
                                ( row["medicine_id".lower()], row["Disease_ID".lower()], row["First_Available_Date".lower()],
                                  row["Effectiveness_Percent".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")

class race_disease_propensity_table():
    def __init__(self, conn_params):
        print("race_disease_propensity_table")
        self.raw_data_layer_table_name = "race_disease_propensity"
        self.curated_layer_table_name = "race_disease_propensity"
        self.conn = psycopg2.connect(**conn_params)

    def extract_from_raw_data(self):
        try:
            query = f"SELECT * FROM {raw_data_layer_schema}.{self.raw_data_layer_table_name};"
            data = sqlio.read_sql_query(query, self.conn)
            return data
        except Exception as e:
            print("Error:", e)

    def __exit__(self):
        # Code to close the connection
        self.conn.close()
    
    def transform_raw_data(self,raw_data):
        try:
            query = f"SELECT * FROM {curated_data_schema}.disease;"
            disease_data = sqlio.read_sql_query(query, self.conn)

            query = f"SELECT * FROM {curated_data_schema}.race;"
            race_data = sqlio.read_sql_query(query, self.conn)
            
        except Exception as e:
            print("Error:", e)

        transformed_data = pd.merge(raw_data, disease_data, how = "inner", left_on = "disease_name",right_on = "disease_name", suffixes=('_x', ''))
        transformed_data = pd.merge(transformed_data, race_data, how = "inner", left_on = "race_description",right_on = "race_description", suffixes=('_x', ''))
        
        
        return transformed_data

    def verify_values_and_clean(self,transformed_data):
        transformed_data = transformed_data
        return transformed_data

    def write_data_to_curated_layer(self,cleaned_transformed_data):

        try:
            print(f"Inserting Values into {self.curated_layer_table_name} Table in {curated_data_schema}")
            cur = self.conn.cursor()
            insert_into_stmt = f" INSERT INTO {curated_data_schema}.{self.curated_layer_table_name} (Race_Code, Disease_ID, Propensity_Value)"
            for _, row in cleaned_transformed_data.iterrows():
                cur.execute(
                                insert_into_stmt + " VALUES (%s, %s, %s);",
                                ( row["Race_Code".lower()], row["Disease_ID".lower()], row["Propensity_Value".lower()])
                )
            self.conn.commit()
            print("Data inserted successfully!")
        except Exception as e:
            print("Error:", e)
        finally:
             cur.close()

    def run_insertion_pipeline(self):
        print("Extracting from Raw Data Layer")
        raw_data = self.extract_from_raw_data()
        print("Transforming data")
        transformed_data = self.transform_raw_data(raw_data)
        print("Standardizing data")
        cleaned_transformed_data = self.verify_values_and_clean(transformed_data)
        print("Writing to curated data layer")
        self.write_data_to_curated_layer(cleaned_transformed_data)
        print("\n------------------------------------------------------\n")



if __name__ == "__main__":
    disease_type_tbl_pipeline = disease_type_table(conn_params)
    disease_type_tbl_pipeline.run_insertion_pipeline()

    race_tbl_pipeline = race_table(conn_params)
    race_tbl_pipeline.run_insertion_pipeline()

    medicine_tbl_pipeline = medicine_table(conn_params)
    medicine_tbl_pipeline.run_insertion_pipeline()

    location_tbl_pipeline = location_table(conn_params)
    location_tbl_pipeline.run_insertion_pipeline()

    disease_tbl_pipeline = disease_table(conn_params)
    disease_tbl_pipeline.run_insertion_pipeline()

    person_tbl_pipeline = person_table(conn_params)
    person_tbl_pipeline.run_insertion_pipeline()

    patient_disease_table_tbl_pipeline = patient_disease_table(conn_params)
    patient_disease_table_tbl_pipeline.run_insertion_pipeline()

    medicine_disease_interaction_table_tbl_pipeline = medicine_disease_interaction_table(conn_params)
    medicine_disease_interaction_table_tbl_pipeline.run_insertion_pipeline()

    race_disease_propensity_tbl_pipeline = race_disease_propensity_table(conn_params)
    race_disease_propensity_tbl_pipeline.run_insertion_pipeline()