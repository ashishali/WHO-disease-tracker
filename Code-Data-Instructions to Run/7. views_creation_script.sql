CREATE OR REPLACE VIEW reporting_layer_DW.Disease_Statistics_By_Location AS
SELECT
    dd.disease_name,
    ld.city_name,
    ld.state_province_name,
    ld.country_name,
    SUM(dpf.num_of_patients) AS total_patients,
    ROUND(AVG(dpf.avg_severity_value), 2) AS avg_severity,
    ROUND(AVG(dpf.race_propensity_value), 2) AS avg_propagation
FROM
    reporting_layer_DW.Diseased_Patient_Fact dpf
    JOIN reporting_layer_DW.Disease_Dimension dd ON dpf.disease_id = dd.disease_id
    JOIN reporting_layer_DW.Location_Dimension ld ON dpf.location_id = ld.location_surrogate_key
GROUP BY
    dd.disease_name,
    ld.city_name,
    ld.state_province_name,
    ld.country_name;


--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
-- Medicine Effectiveness by Disease
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting_layer_DW.Medicine_Effectiveness_By_Disease AS
SELECT
    dd.disease_name,
    md.manufacturer_name,
    SUM(mf.num_of_medicines) AS total_medicines,
    ROUND(AVG(mf.avg_effectiveness_of_medicines), 2) AS avg_effectiveness
FROM
    reporting_layer_DW.Medicine_Fact mf
    JOIN reporting_layer_DW.Disease_Dimension dd ON mf.disease_id = dd.disease_id
    JOIN reporting_layer_DW.Manufacturer_Dimension md ON mf.manufacturer_id = md.manufacturer_surrogate_key
GROUP BY
    dd.disease_name,
    md.manufacturer_name;


-- no data cuz i didn't create the data for effectiveness!!
--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
-- Monthly Trends of Disease Severity
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting_layer_DW.Monthly_Disease_Severity_Trends AS
SELECT
    dd.disease_name,
    dd.intensity_level,
    dd.disease_type_cd,
    DATE_TRUNC('month', dpf.start_of_month_date) AS month,
    ROUND(AVG(dpf.avg_severity_value), 2) AS avg_severity
FROM
    reporting_layer_DW.Diseased_Patient_Fact dpf
    JOIN reporting_layer_DW.Disease_Dimension dd ON dpf.disease_id = dd.disease_id
GROUP BY
    dd.disease_name,
    dd.intensity_level,
    dd.disease_type_cd,
    DATE_TRUNC('month', dpf.start_of_month_date)
ORDER BY
    month, dd.disease_name;

--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
-- Active Locations for Specific Diseases
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting_layer_DW.Active_Locations_By_Disease AS
SELECT
    dd.disease_name,
    ld.city_name,
    ld.state_province_name,
    ld.country_name,
    COUNT(dpf.location_id) AS num_of_reports
FROM
    reporting_layer_DW.Diseased_Patient_Fact dpf
    JOIN reporting_layer_DW.Disease_Dimension dd ON dpf.disease_id = dd.disease_id
    JOIN reporting_layer_DW.Location_Dimension ld ON dpf.location_id = ld.location_surrogate_key
GROUP BY
    dd.disease_name,
    ld.city_name,
    ld.state_province_name,
    ld.country_name
ORDER BY
    dd.disease_name, num_of_reports DESC;

--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
-- Effectiveness of Medicines Across Manufacturers
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting_layer_DW.Manufacturer_Medicine_Effectiveness AS
SELECT
    md.manufacturer_name,
    SUM(mf.num_of_medicines) AS total_medicines_produced,
    ROUND(AVG(mf.avg_effectiveness_of_medicines), 2) AS avg_medicine_effectiveness
FROM
    reporting_layer_DW.Medicine_Fact mf
    JOIN reporting_layer_DW.Manufacturer_Dimension md ON mf.manufacturer_id = md.manufacturer_surrogate_key
GROUP BY
    md.manufacturer_name
ORDER BY
    avg_medicine_effectiveness DESC;

-- no data cuz I did not create any value for effectiveness
--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
-- Disease Distribution by Gender and Race
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW reporting_layer_DW.Disease_Distribution_By_Gender_Race AS
SELECT
    dd.disease_name,
    rgd.race_description,
    rgd.gender,
    COUNT(dpf.combination_id) AS num_of_reports
FROM
    reporting_layer_DW.Diseased_Patient_Fact dpf
    JOIN reporting_layer_DW.Disease_Dimension dd ON dpf.disease_id = dd.disease_id
    JOIN reporting_layer_DW.Race_Gender_Dimension rgd ON dpf.combination_id = rgd.combination_id
GROUP BY
    dd.disease_name,
    rgd.race_description,
    rgd.gender
ORDER BY
    dd.disease_name, num_of_reports DESC;

--------------------------------------------------------------------------------------------------------------------------------------------
