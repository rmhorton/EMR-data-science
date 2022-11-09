-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Explore Synthea data
-- MAGIC 
-- MAGIC Reference for [Spark SQL built-in functions](https://spark.apache.org/docs/latest/api/sql/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Examine the filesystem(s)
-- MAGIC 
-- MAGIC Local vs. Distributed

-- COMMAND ----------

-- MAGIC %md ### Local files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import os
-- MAGIC 
-- MAGIC os.getcwd()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC os.listdir()

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls -alh

-- COMMAND ----------

-- MAGIC %md ### Distributed filesystem

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /FileStore

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore the database

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC -- this should fail if you haven't selected the right database
-- MAGIC 
-- MAGIC select * from encounters

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show databases

-- COMMAND ----------

use emr_sample;

show tables;

-- COMMAND ----------

select * from encounters;

-- COMMAND ----------

desc encounters

-- COMMAND ----------

select * from patients

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit_: What is the total number of patients?

-- COMMAND ----------

select speciality, count(*) tally from providers group by speciality

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC tables = spark.sql("show tables").toPandas()
-- MAGIC synthea_tables = tables[tables.database == 'emr_sample']['tableName'].values
-- MAGIC 
-- MAGIC for syntab in synthea_tables:
-- MAGIC   print(f'{syntab}')
-- MAGIC   print(spark.sql(f'describe table {syntab}').toPandas())

-- COMMAND ----------

-- DBTITLE 1,Encounters
select * from encounters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit:_ What was the date of the most recent encounter for each patient?

-- COMMAND ----------

select count(*) tally, code, collect_set(description) description_list from observations group by code order by size(description_list) desc

-- COMMAND ----------

select count(*) tally, code, collect_set(description) description_list from conditions group by code order by size(description_list) desc


-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # 72514-3 "Pain severity - 0-10 verbal numeric rating [Score] - Reported"
-- MAGIC 
-- MAGIC sql = "select int(value) pain_level, count(*) tally from observations where code = '72514-3' group by pain_level order by pain_level"
-- MAGIC 
-- MAGIC display(spark.sql(sql).toPandas().plot.bar(x='pain_level', y='tally'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit:_ How would you discover observations related to 'pain'?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit:_ What are the different kinds of encounters, and how many of each are in the database?

-- COMMAND ----------

-- DBTITLE 1,Age at onset
select p.first, p.last, c.description condition,
        floor(datediff(date(c.start), date(p.birthdate))/365.24) age_at_onset 
    from conditions c join patients p on c.patient = p.id

-- COMMAND ----------

select c.description, count(*) from conditions c where lower(c.description) rlike('breast') group by c.description
-- conditions: 'Pathological fracture due to osteoporosis (disorder)'' 1026
-- observations: 'DXA [T-score] Bone density'
-- select c.* from conditions c where c.description = 'Malignant neoplasm of breast (disorder)'

-- 'Whiplash injury to neck' 5161
-- 'Dislocation of hip joint (disorder)' 63
-- 'Osteoarthritis of hip' 1657
-- 'Closed fracture of hip' 948

-- COMMAND ----------

-- select description, value from observations where description rlike('DXA')
-- show tables -- medications, procedures, conditions, observations

select p.gender, cast(o.value as float) T_score from observations o join patients p on o.patient = p.id where o.description == 'DXA [T-score] Bone density'

-- COMMAND ----------

-- DBTITLE 1,Medications
select * from medications limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Plots in R

-- COMMAND ----------

-- MAGIC %r
-- MAGIC options(repr.plot.width=600, repr.plot.height=1200)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC library(dplyr)
-- MAGIC library(sparklyr)
-- MAGIC library(ggplot2)
-- MAGIC 
-- MAGIC sc <- spark_connect(method = "databricks")
-- MAGIC 
-- MAGIC conditions <- c('Dislocation of hip joint (disorder)', 'Closed fracture of hip', 'Osteoarthritis of hip', 'Malignant neoplasm of breast (disorder)')
-- MAGIC 
-- MAGIC sql <- sprintf("select p.first, p.last, p.gender, c.description condition,
-- MAGIC                         floor(datediff(date(c.start), date(p.birthdate))/365.24) age_at_onset 
-- MAGIC                   from conditions c join patients p 
-- MAGIC                         on c.patient = p.id 
-- MAGIC                   where c.description in ('%s')", paste(conditions, collapse="','"))
-- MAGIC 
-- MAGIC 
-- MAGIC sdf_sql(sc, sql) %>% ggplot(aes(x=age_at_onset, fill=gender)) + geom_density(alpha=0.5) + facet_grid(condition ~ ., scales='free_y')

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC sdf_sql(sc, sql) %>% 
-- MAGIC   ggplot(aes(x=age_at_onset, fill=gender)) + geom_histogram(binwidth = 1) + facet_grid(condition ~ gender, scales='free_y')

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC library(dplyr)
-- MAGIC library(sparklyr)
-- MAGIC library(ggplot2)
-- MAGIC 
-- MAGIC sc <- spark_connect(method = "databricks")
-- MAGIC sql <- "select description, cast(value as float) T_score from observations where description == 'DXA [T-score] Bone density'"
-- MAGIC 
-- MAGIC bone_density <- sdf_sql(sc, sql)
-- MAGIC bone_density %>% ggplot(aes(x=T_score)) + geom_density(fill='blue', alpha=0.5)

-- COMMAND ----------

-- select max(cast(value as float)) max_T_score from observations where description == 'DXA [T-score] Bone density'

select value T_score, count(*) tally from observations where description == 'DXA [T-score] Bone density' group by value order by cast(value as float)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC sql <- "select p.gender, cast(o.value as float) T_score from observations o join patients p on o.patient = p.id where o.description == 'DXA [T-score] Bone density'"
-- MAGIC sdf_sql(sc, sql) %>% ggplot(aes(x=T_score, fill=gender)) + geom_density(alpha=0.5)

-- COMMAND ----------

-- select * from encounters limit 10

select code, collect_set(description), count(*) tally from encounters group by code order by tally desc -- encounterclass

-- 'General examination of patient (procedure)', "Encounter for 'check-up'"

-- 162673000   General examination of patient (procedure)
-- 185349003   Encounter for check up (procedure)

-- COMMAND ----------

-- select count(*) from conditions where code = 254837009 -- 1369

with
encounter_condition as (
  select e.id encounter, e.code encounter_code, collect_set(c.code) as condition_code_list
    from encounters e join conditions c on c.encounter = e.id
    -- where e.code in ('162673000', '185349003')
    group by e.id, e.code
),
encounter_bcadx as (
  select encounter, encounter_code, case when array_contains(condition_code_list, '254837009') then 'Y' else 'N' end as breast_cancer_dx 
    from encounter_condition
)
select * from encounter_bcadx where breast_cancer_dx = 'Y'
-- select breast_cancer_dx, count(*) tally from encounter_bcadx group by breast_cancer_dx

-- COMMAND ----------

-- select * from conditions where code = 254837009
-- code 254837009 'Malignant neoplasm of breast (disorder)'

-- select * from conditions where code = 254837009 -- and stop is not null

with
encounter_condition as (
  select e.id, collect_set(c.code) as condition_code_list
    from encounters e join conditions c on c.encounter = e.id
    where e.code in ('162673000', '185349003')
    group by e.id
)
select * from encounter_condition where array_contains(condition_code_list, '254837009')

-- COMMAND ----------


