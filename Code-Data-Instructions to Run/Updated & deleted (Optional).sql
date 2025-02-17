-- 1. The intensity level of a specific disease(i.e Anxiety Disorders) has been re-evaluated and needs to be updated.

--Data Before Update
SELECT * FROM curated_layer.Disease WHERE Disease_ID = 7;

--Data After Update
UPDATE curated_layer.Disease
SET Intensity_Level = 8  -- New Intensity_Level
WHERE Disease_ID = 7;
SELECT * FROM curated_layer.Disease WHERE Disease_ID = 7;


--2. A person has moved to a new city, so their Primary_Location_ID needs to be updated.

-- Data Before Update
SELECT * FROM curated_layer.Person WHERE Person_ID = 145;

-- Data After Update
UPDATE curated_layer.Person
SET Primary_Location_ID = 30938  -- New location_ID
WHERE Person_ID = 145;
SELECT * FROM curated_layer.Person WHERE Person_ID = 145;


-- 3. The manufacturer of a medicine changed, and the Manufacturer_Name needs to be updated accordingly.

-- Data Before Update
SELECT * FROM curated_layer.Medicine WHERE Medicine_ID = 101;

--Data After Update
UPDATE curated_layer.Medicine
SET Manufacturer_Name = 'Novartis' -- New Manufacturer_Name
WHERE Medicine_ID = 101;
SELECT * FROM curated_layer.Medicine WHERE Medicine_ID = 101;

-- 4. A person is no longer part of the system and needs to be removed.
BEGIN;

-- Data Before Delete
SELECT * FROM curated_layer.Person Where Person_ID = 888;
SELECT * FROM curated_layer.Diseased_Patient WHERE Patient_ID = 888;  

-- Data After Update
DELETE FROM curated_layer.Person
WHERE Person_ID = 888;



-- Verify the deletion in the Diseased_Patient table
SELECT * FROM curated_layer.Person Where Person_ID = 888;

SELECT * FROM curated_layer.Diseased_Patient
WHERE Patient_ID = 888;  

ROLLBACK;

--5. A medicine is discontinued and must be deleted from the system.

-- Data Before Delete
SELECT * FROM curated_layer.Medicine  WHERE Medicine_ID = 134;

-- Data After Update
DELETE FROM curated_layer.Medicine
WHERE Medicine_ID = 101;
SELECT * FROM curated_layer.Medicine  WHERE Medicine_ID = 101;

-- Verify the deletion in the Medicine_Disease_Interaction table
SELECT Medicine_ID, Disease_ID, First_Available_Date
FROM curated_layer.Medicine_Disease_Interaction
WHERE Medicine_ID = 101;  

-- Race code update

SELECT * FROM curated_layer.Race WHERE race_code = 'ASN';

-- Data After Update
UPDATE curated_layer.Race SET race_code = 'ASI'
WHERE race_code = 'ASN';

SELECT * FROM curated_layer.Race WHERE race_code = 'ASN';

-- Verify the update in the Disease table
SELECT *
FROM curated_layer.Person
WHERE race_cd = 'ASI';
