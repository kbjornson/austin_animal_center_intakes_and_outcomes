/******************************************************
Austin Animal Center Intakes and Outcomes

Date: 09/22/2025
Author: kbjornson
Purpose: Combine animal intakes and animal outcomes datasets to see the full picture 
		 of an animal's stay and outcome after being taken to the shelter

******************************************************/
DROP TABLE IF EXISTS AnimalIntake;

CREATE TABLE AnimalIntake (
    animal_id VARCHAR(50),
    name VARCHAR(100),
	datetime DATETIME,
	monthyear VARCHAR(20),
	intake_type VARCHAR(50),
    intake_condition VARCHAR(50),
    animal_type VARCHAR(50),
    sex_upon_intake VARCHAR(50),
    age_upon_intake VARCHAR(50),
	breed VARCHAR(100),
    color VARCHAR(100)
);

BULK INSERT AnimalIntake
FROM 'C:/Users/bjorn/Documents/Austin_Animal_Center_Project/Austin_Animal_Center_Intakes_Cleaned.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- drop intakes table and drop columns in python that I'm not using and reimport 
-- check distinct animal type

with intake_seq as (
	select *,
		row_number() over (partition by animal_id order by cast(datetime as date)) as seq
	from AnimalShelterDB.dbo.AnimalIntake
	),

outcome_seq as (
	select *,
		row_number() over (partition by animal_id order by date) as seq
	from AnimalShelterDB.dbo.AnimalOutcome
	)
		
select
	i.animal_id
	, i.name
	, i.animal_type
	, i.breed
	, i.color
	, cast(i.datetime as date) as intake_date
	, o.date as outcome_date
	, datediff(day, cast(i.datetime as date), o.date) as days_in_shelter
	, i.monthyear as intake_monthyear
	, o.monthyear as outcome_monthyear
	, i.intake_type
	, i.intake_condition
	, o.outcome_type
	, o.outcome_subtype
	, i.sex_upon_intake
	, o.sex_upon_outcome
	, case 
		when i.sex_upon_intake = 'Intact Male' and o.sex_upon_outcome = 'Neutered Male' then 'Y'
		when i.sex_upon_intake = 'Intact Female' and o.sex_upon_outcome = 'Spayed Female' then 'Y'
		else 'N'
	  end as neutered_at_shelter
	, i.age_upon_intake
	, o.age_upon_outcome
from intake_seq i
left join outcome_seq o
	on i.animal_id = o.animal_id
	and i.seq = o.seq
where o.date >= cast(i.datetime as date)

--order by i.animal_id, cast(i.datetime as date)
;

select animal_id, cast(datetime as date) from AnimalShelterDB.dbo.AnimalIntake
where animal_id = 'A660279';

select animal_id, date from AnimalShelterDB.dbo.AnimalOutcome
where animal_id = 'A660279';