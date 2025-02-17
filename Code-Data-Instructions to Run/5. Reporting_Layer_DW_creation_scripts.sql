drop view IF EXISTS reporting_layer_DW.Disease_Statistics_By_Location; 
drop view IF EXISTS reporting_layer_DW.Medicine_Effectiveness_By_Disease;
drop view IF EXISTS reporting_layer_DW.Monthly_Disease_Severity_Trends;
drop view IF EXISTS reporting_layer_DW.Active_Locations_By_Disease;
drop view IF EXISTS reporting_layer_DW.Manufacturer_Medicine_Effectiveness;
drop view IF EXISTS reporting_layer_DW.Disease_Distribution_By_Gender_Race;

DROP TABLE IF EXISTS reporting_layer_DW.Medicine_Fact;
DROP TABLE IF EXISTS reporting_layer_DW.Diseased_Patient_Fact;
DROP TABLE IF EXISTS reporting_layer_DW.Manufacturer_Dimension;
DROP TABLE IF EXISTS reporting_layer_DW.Race_Gender_Dimension;
DROP TABLE IF EXISTS reporting_layer_DW.Date_Dimension;
DROP TABLE IF EXISTS reporting_layer_DW.Location_Dimension;
DROP TABLE IF EXISTS reporting_layer_DW.Disease_Dimension;
DROP TABLE IF EXISTS reporting_layer_DW.Disease_Type_Dimension;





CREATE TABLE reporting_layer_DW.Disease_Type_Dimension (
    disease_type_cd VARCHAR(5) PRIMARY KEY, 
    disease_type_description VARCHAR(100) 
);

CREATE TABLE reporting_layer_DW.Disease_Dimension (
    disease_id INT PRIMARY KEY, 
    disease_name VARCHAR(100) NOT NULL, 
    disease_type_cd VARCHAR(5) NOT NULL, -- Links to Disease_Type_Dimension
    intensity_level INT CHECK (intensity_level BETWEEN 1 AND 10),
	CONSTRAINT fk_disease_type FOREIGN KEY (disease_type_cd) REFERENCES reporting_layer_DW.Disease_Type_Dimension (disease_type_cd)
);


CREATE TABLE reporting_layer_DW.Location_Dimension (
    location_surrogate_key Serial PRIMARY KEY, -- Surrogate key for SCD
    location_id INT NOT NULL, 
    city_name VARCHAR(100), 
    state_province_name VARCHAR(100), 
    country_name VARCHAR(100), 
    developing_flag VARCHAR(1) CHECK (developing_flag IN ('Y', 'N')), 
    wealth_rank_number INT CHECK (wealth_rank_number BETWEEN 1 AND 10), 
    effective_start_date DATE DEFAULT '1899-01-01', 
    effective_end_date DATE DEFAULT NULL, 
    is_current_flag VARCHAR(1) CHECK (is_current_flag IN ('Y', 'N')),
	CONSTRAINT unique_location_id_effective_start_end_date
		UNIQUE (location_id, effective_start_date, effective_end_date) 
);


CREATE TABLE reporting_layer_DW.Date_Dimension (
    date_key INT PRIMARY KEY, 
    full_date DATE UNIQUE NOT NULL, 
    day INT, 
    month INT, 
    year INT, 
    quarter INT, 
    week INT, 
    day_name VARCHAR(20), 
    month_name VARCHAR(20) 
);


CREATE TABLE reporting_layer_DW.Race_Gender_Dimension (
    combination_id INT PRIMARY KEY, 
    race_code VARCHAR(5) NOT NULL, 
    race_description VARCHAR(100), 
    gender VARCHAR(1) CHECK (gender IN ('M', 'F', 'O', 'U')) 
);


CREATE TABLE reporting_layer_DW.Manufacturer_Dimension (
    manufacturer_surrogate_key Serial PRIMARY KEY, -- Surrogate key FOR SCD 
    manufacturer_id INT NOT NULL, 
    manufacturer_name VARCHAR(150) NOT NULL, 
    effective_start_date DATE DEFAULT '1899-01-01', 
    effective_end_date DATE DEFAULT NULL, 
    is_current_flag VARCHAR(1) CHECK (is_current_flag IN ('Y', 'N')),
	CONSTRAINT unique_manufacturer_id_effective_start_end_date
		UNIQUE (manufacturer_id, effective_start_date, effective_end_date) 
);



-- Diseased_Patient_Fact Table
CREATE TABLE reporting_layer_DW.Diseased_Patient_Fact (
    disease_id INT NOT NULL, -- Links to Disease_Dimension
    location_id INT NOT NULL, -- Links to Location_Dimension
    combination_id INT NOT NULL, -- Links to Race_Gender_Dimension
    num_of_patients INT NOT NULL, 
    race_propensity_value DECIMAL(5,2), 
    avg_severity_value DECIMAL(5,2), 
    start_of_month_date DATE NOT NULL, -- Links to Date_Dimension
	PRIMARY KEY(disease_id, location_id, combination_id, start_of_month_date),
    FOREIGN KEY (disease_id) REFERENCES reporting_layer_DW.Disease_Dimension(disease_id),
    FOREIGN KEY (location_id) REFERENCES reporting_layer_DW.Location_Dimension(location_surrogate_key),
    FOREIGN KEY (combination_id) REFERENCES reporting_layer_DW.Race_Gender_Dimension(combination_id),
    FOREIGN KEY (start_of_month_date) REFERENCES reporting_layer_DW.Date_Dimension(full_date)
);


-- Medicine_Fact Table
CREATE TABLE reporting_layer_DW.Medicine_Fact (
    fact_id Serial PRIMARY KEY, -- Surrogate key for this fact table
    disease_id INT NOT NULL, -- Links to Disease_Dimension
	First_Available_Date DATE NOT NULL,
    manufacturer_id INT, -- Links to Manufacturer_Dimension's surrogate key
    num_of_medicines INT, -- Measure: number of medicines used
    avg_effectiveness_of_medicines DECIMAL(5,2), -- Measure: effectiveness of medicines
    FOREIGN KEY (disease_id) REFERENCES reporting_layer_DW.Disease_Dimension(disease_id),
    FOREIGN KEY (manufacturer_id) REFERENCES reporting_layer_DW.Manufacturer_Dimension(manufacturer_surrogate_key),
	FOREIGN KEY (First_Available_Date) REFERENCES reporting_layer_DW.Date_Dimension(full_date)
);
