-- Disease Source System
DROP TABLE IF EXISTS raw_data_staging_layer.Disease;
DROP TABLE IF EXISTS raw_data_staging_layer.Disease_Type;

-- Medicine Source System
DROP TABLE IF EXISTS raw_data_staging_layer.Medicine_Disease_Interaction;
DROP TABLE IF EXISTS raw_data_staging_layer.Medicine;


-- Patient Source System
DROP TABLE IF EXISTS raw_data_staging_layer.Diseased_Patient;
DROP TABLE IF EXISTS raw_data_staging_layer.Person;
DROP TABLE IF EXISTS raw_data_staging_layer.Location;
DROP TABLE IF EXISTS raw_data_staging_layer.Country_index;
DROP TABLE IF EXISTS raw_data_staging_layer.Race;


-- Race_Disease_Propensity Source System (WHO Internal Report)
DROP TABLE IF EXISTS raw_data_staging_layer.Race_Disease_Propensity;




------------------------------------------------------------------------------------------------------------
-- Raw Data Tables
------------------------------------------------------------------------------------------------------------

-- Disease Source System
-------------------------

CREATE TABLE raw_data_staging_layer.Disease_Type (
    Disease_Type_Code VARCHAR(5) PRIMARY KEY,
	Disease_Type_Name VARCHAR(40),
    Disease_Type_Description VARCHAR(1000) NOT NULL
);

CREATE TABLE raw_data_staging_layer.Disease (
    Disease_ID INT PRIMARY KEY,
    Disease_Name VARCHAR(100) NOT NULL,
    Intensity_Level_Qty INT,
    Disease_Type_Cd VARCHAR(50) NOT NULL,
    Source_Disease_Cd INT
);

------------------------------------------------------------------------------------------------------------

-- Medicine Source System
-------------------------


CREATE TABLE raw_data_staging_layer.Medicine (
    Medicine_ID INT PRIMARY KEY,
    Medicine_Name VARCHAR(250),
    Standard_Industry_Number VARCHAR(25),
    Company VARCHAR(150),
    Active_Ingredient_Name VARCHAR(150)
);


CREATE TABLE raw_data_staging_layer.Medicine_Disease_Interaction (
    row_ID INT PRIMARY KEY,
    Disease_Code INT NOT NULL,
    Disease_Name VARCHAR(250) NOT NULL,
    Medicine_ID INT NOT NULL,
    First_Available_Date DATE,
    Effectiveness_Percent FLOAT,
    CONSTRAINT fk_Medicine_Medicine_Disease_Interaction FOREIGN KEY (Medicine_ID) REFERENCES raw_data_staging_layer.Medicine(Medicine_ID)
);


------------------------------------------------------------------------------------------------------------

-- Patient Source System
-------------------------

CREATE TABLE raw_data_staging_layer.Location (
    City_ID INT PRIMARY KEY,
    City_Name VARCHAR(100) NOT NULL,
    State_Province_Name VARCHAR(100),
    Country_Name VARCHAR(100) NOT NULL
);

CREATE TABLE raw_data_staging_layer.Country_index (
    Country_ID SERIAL PRIMARY KEY ,
    Country_Name VARCHAR(100) NOT NULL,
    Wealth_Rank_Number INT NOT NULL,
    Developing_Flag CHAR(1)
);

CREATE TABLE raw_data_staging_layer.Race (
    Race_Code VARCHAR(5) PRIMARY KEY,
    Race_Description VARCHAR(100)
);

CREATE TABLE raw_data_staging_layer.Person (
    Person_ID SERIAL PRIMARY KEY,
    Last_Name VARCHAR(50) NOT NULL,
    First_Name VARCHAR(50),
    Gender CHAR(1) NOT NULL,
    Race_Code VARCHAR(5) NOT NULL,
    Location_ID INT,
    CONSTRAINT fk_Race_Person FOREIGN KEY (Race_Code) REFERENCES raw_data_staging_layer.Race(Race_Code),
    CONSTRAINT fk_Location_Person FOREIGN KEY (Location_ID) REFERENCES raw_data_staging_layer.Location(City_ID)
);


CREATE TABLE raw_data_staging_layer.Diseased_Patient (
    Person_ID INT NOT NULL,
    Disease_Code INT NOT NULL,
    Disease_Name VARCHAR(250) NOT NULL,
    Severity INT,
    start_date DATE NOT NULL,
    end_date DATE,
    PRIMARY KEY(Person_ID, Disease_Code, start_date),
    CONSTRAINT fk_Diseased_Patient_Person FOREIGN KEY (Person_ID) REFERENCES raw_data_staging_layer.Person(Person_ID)
	);


------------------------------------------------------------------------------------------------------------

-- Race_Disease_Propensity Source System (WHO Internal Report)
-------------------------


CREATE TABLE raw_data_staging_layer.Race_Disease_Propensity (
    Race_Code VARCHAR(5),
    Race_Description VARCHAR(100),
    Disease_ID INT NOT NULL,
    Disease_Name VARCHAR(400),
    Propensity_Value INT NOT NULL,
    PRIMARY KEY (Race_Code, Disease_ID)
);
