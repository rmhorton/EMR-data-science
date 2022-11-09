-- What is the total number of patients?
select count(distinct id) from patients;
;

-- What was the date of the most recent encounter for each patient?
select patient, max(date(START)) most_recent_encounter from encounters group by patient 
;

-- How would you discover observations related to 'pain'?
select description, count(*) tally from observations where lower(description) rlike 'pain' group by description
;

--- What are the different kinds of encounters, and how many of each are in the database?
select encounterclass, count(*) tally from encounters group by encounterclass order by tally desc
;

-- What is the most common medication and dose?
select description, count(*) tally from medications group by description order by tally desc
;

-- What is the most common disorder treated by medication?
select reasondescription, count(*) tally from medications group by reasondescription order by tally desc
;

--- What are the most common prescriptions for hypertension?
select description, count(*) tally from medications where reasondescription == 'Hypertension' group by description order by tally desc
;

How would you get only the latest measurement for each patient?
