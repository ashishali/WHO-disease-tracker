DROP TABLE IF EXISTS curated_layer.Diseased_Patient;
DROP TABLE IF EXISTS curated_layer.Person;
DROP TABLE IF EXISTS curated_layer.Race_Disease_Propensity;
DROP TABLE IF EXISTS curated_layer.Medicine_Disease_Interaction;
DROP TABLE IF EXISTS curated_layer.Disease;
DROP TABLE IF EXISTS curated_layer.Disease_Type;
DROP TABLE IF EXISTS curated_layer.Race;
DROP TABLE IF EXISTS curated_layer.Medicine;
DROP TABLE IF EXISTS curated_layer.Location;
DROP TABLE IF EXISTS curated_layer.Audit_Log;
DROP FUNCTION IF EXISTS curated_layer.audit_tf;

-- Disease Type Table.
CREATE TABLE curated_layer.Disease_Type (
    Disease_Type_Code VARCHAR(5) PRIMARY KEY,
    Disease_Type_Name VARCHAR(40) NOT NULL,
    Disease_Type_Description VARCHAR(1000)
);

-- Race Table.
CREATE TABLE curated_layer.Race (
    Race_Code VARCHAR(5) PRIMARY KEY,
    Race_Description VARCHAR(100) NOT NULL
);

-- Medicine Table.

CREATE TABLE curated_layer.Medicine (
    Medicine_ID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    Standard_Industry_Number VARCHAR(25),
    Name VARCHAR(250) NOT NULL,
    Manufacturer_id INT,
    Manufacturer_Name VARCHAR(150),
    Active_Ingredient_Name VARCHAR(150)
);

-- Location Table.
CREATE TABLE curated_layer.Location (
    Location_ID INT PRIMARY KEY,
    City_Name VARCHAR(100) NOT NULL,
    State_Province_Name VARCHAR(100),
    Country_Name VARCHAR(100) NOT NULL,
    Developing_Flag CHAR(1) NOT NULL CHECK (Developing_Flag IN ('Y', 'N')),
    Wealth_Rank_Number INT NOT NULL CHECK (Wealth_Rank_Number BETWEEN 1 AND 10)
);

-- Disease Table.
CREATE TABLE curated_layer.Disease (
    Disease_ID INT PRIMARY KEY,
    Disease_Name VARCHAR(100) NOT NULL,
    Intensity_Level INT DEFAULT 1 CHECK (Intensity_Level BETWEEN 1 AND 10),
    Disease_Type_Cd CHAR(10) NOT NULL,
    Source_Disease_Cd INT,
    FOREIGN KEY (Disease_Type_Cd) REFERENCES curated_layer.Disease_Type(Disease_Type_Code),
    FOREIGN KEY (Source_Disease_Cd) REFERENCES curated_layer.Disease(Disease_ID)
);

-- Medicine_Disease_Interaction Table.
CREATE TABLE curated_layer.Medicine_Disease_Interaction (
    Medicine_ID INT NOT NULL,
    Disease_ID INT NOT NULL,
    First_Available_Date DATE,
    Effectiveness_Percent FLOAT CHECK (Effectiveness_Percent BETWEEN 0 AND 100),
	PRIMARY KEY(Medicine_ID, Disease_ID),
    FOREIGN KEY (Disease_ID) REFERENCES curated_layer.Disease(Disease_ID),
    FOREIGN KEY (Medicine_ID) REFERENCES curated_layer.Medicine(Medicine_ID) ON DELETE CASCADE
);

-- Race Disease Propensity Table.
CREATE TABLE curated_layer.Race_Disease_Propensity (
    Race_Code VARCHAR(5) NOT NULL,
    Disease_ID INT NOT NULL,
    Propensity_Value INT NOT NULL CHECK (Propensity_Value BETWEEN 1 AND 10),
    PRIMARY KEY (Race_Code, Disease_ID),
    FOREIGN KEY (Race_Code) REFERENCES curated_layer.Race(Race_Code) ON UPDATE CASCADE,
    FOREIGN KEY (Disease_ID) REFERENCES curated_layer.Disease(Disease_ID)
);


-- Person Table.
CREATE TABLE curated_layer.Person (
    Person_ID INT PRIMARY KEY,
    Last_Name VARCHAR(50) NOT NULL,
    First_Name VARCHAR(50),
    Gender CHAR(1) NOT NULL CHECK (Gender IN ('M', 'F', 'O','U')),
    Primary_Location_ID INT,
    Race_CD VARCHAR(5) NOT NULL,
    FOREIGN KEY (Primary_Location_ID) REFERENCES curated_layer.Location(Location_ID),
    FOREIGN KEY (Race_CD) REFERENCES curated_layer.Race(Race_Code) ON UPDATE CASCADE
);

-- Diseased Patient Table.
CREATE TABLE curated_layer.Diseased_Patient (
    Patient_ID INT NOT NULL,
    Disease_ID INT NOT NULL,
    Severity_Value INT NOT NULL DEFAULT 1 CHECK (Severity_Value BETWEEN 1 AND 10),
    Start_Date DATE NOT NULL,
    End_Date DATE,
    PRIMARY KEY (Patient_ID, Disease_ID, start_date),
    FOREIGN KEY (Patient_ID) REFERENCES curated_layer.Person(Person_ID) ON DELETE CASCADE,
    FOREIGN KEY (Disease_ID) REFERENCES curated_layer.Disease(Disease_ID)
);


-- AUDIT Log Table
CREATE TABLE curated_layer.Audit_log
(
 src_table TEXT NOT NULL,
 v_old TEXT,
 v_new TEXT,
 "user" TEXT NOT NULL DEFAULT current_user,
 "action" TEXT NOT NULL,
 action_time TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);




CREATE OR REPLACE FUNCTION curated_layer.audit_tf() RETURNS TRIGGER LANGUAGE plpgsql as 
$$
DECLARE 
  k text;
  v text;
  col_list text;
  j_new jsonb := to_jsonb(new);
  j_old jsonb := to_jsonb(old);
BEGIN
    IF TG_OP = 'INSERT' THEN -- only shows new values
		INSERT INTO curated_layer.audit_log (src_table, v_new, action) 
            VALUES (TG_TABLE_NAME, j_new, TG_OP);
    ELSIF TG_OP = 'UPDATE' THEN -- shows new and old values 
       
		INSERT INTO curated_layer.audit_log (src_table, v_new, v_old, action) 
		    VALUES (TG_TABLE_NAME, j_new, j_old, TG_OP);

    ELSIF TG_OP = 'DELETE' THEN -- only shows old values
		INSERT INTO curated_layer.audit_log (src_table, v_old, action) 
		    VALUES (TG_TABLE_NAME, j_old, TG_OP);
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER location_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.location
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();


CREATE OR REPLACE TRIGGER disease_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.disease
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER disease_type_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.disease_type
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER diseased_patient_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.diseased_patient
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER medicine_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.medicine
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER medicine_disease_interaction_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.medicine_disease_interaction
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER person_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.person
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER race_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.race
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();

CREATE OR REPLACE TRIGGER race_disease_propensity_audit_trigger
AFTER INSERT OR UPDATE or DELETE ON curated_layer.race_disease_propensity
FOR EACH ROW EXECUTE PROCEDURE curated_layer.audit_tf();



