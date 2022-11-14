-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC # displayHTML(f'''
-- MAGIC # <iframe
-- MAGIC #   src="https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fraw.githubusercontent.com%2Frmhorton%2FEMR-data-science%2Fmain%2FML_with_simulated_EMR.pptx&wdSlideIndex=14"
-- MAGIC #   frameborder="0"
-- MAGIC #   width="80%"
-- MAGIC #   height="640"
-- MAGIC   
-- MAGIC # ></iframe>
-- MAGIC # ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Explore Synthea data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Databricks Magic Commands
-- MAGIC To explore data a useful feature of databricks notebook is the magic commands that can come at the beginning of each cell and change the interpretation of the cell content.
-- MAGIC 
-- MAGIC **Mixed Languages**
-- MAGIC The default language of this notebook is SQL. However, you can easily switch to other languages by magic commands: "%python", "%R", "%SQL", "%scala"
-- MAGIC 
-- MAGIC **Auxiliary Cells**
-- MAGIC - "%md": mark-down 
-- MAGIC - "%sh": run shell code
-- MAGIC - "%fs": run dbutils filesystem commands; e.g. _%fs ls_ instead of _"dbutils.fs.ls"_
-- MAGIC 
-- MAGIC More information on [Databricks Notebook Utilities](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Examine the filesystem(s)
-- MAGIC A databricks cluster have a driver node and potentially multiple worker nodes. The file root path on databricks depends the code executed; whether it is executed locally or on a distributed cluster. 
-- MAGIC 
-- MAGIC This is because the cluster is dealing with two filesystems:
-- MAGIC - Local filesystem (e.g., driver's)
-- MAGIC - Distributed DBFS filesystem
-- MAGIC 
-- MAGIC 
-- MAGIC Below you can see & examine the commands and their default filesystems & root-path. You can also learn more about [Accessing Files on Databricks](https://docs.databricks.com/files/index.html).

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### Local driver filesystem
-- MAGIC The block storage volume attached to the driver is the root path for code executed locally. These include command-types:
-- MAGIC - %sh
-- MAGIC - Most Python code (not PySpark)
-- MAGIC - Most Scala code (not Spark)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you are running the notebook from the "Repos", your current directory is the 'EMR-data-science'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import os
-- MAGIC os.getcwd()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While your root directory shows the driver file-system root directory:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC os.listdir("/")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Similarly, %sh codes are executed locally:

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls -alh

-- COMMAND ----------

--%sh
--ls /dbfs/

-- COMMAND ----------

--%fs
--ls file:/

-- COMMAND ----------

-- MAGIC %md ### Distributed filesystem
-- MAGIC The DBFS root is the root path for Spark and DBFS commands. These include command-types:
-- MAGIC - Spark SQL
-- MAGIC - DataFrames
-- MAGIC - dbutils.fs
-- MAGIC - %fs
-- MAGIC 
-- MAGIC By default these commands are executed in a distributed fashion, and their default filesystem is DBFS.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /FileStore/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC On previous notebook, we uploaded our data to DBFS as a relational database "emr_sample". We can see the corresponding files on the dbfs hive directory as below:

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /user/hive/warehouse/emr_sample.db/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore the database
-- MAGIC This section we use SQL queries and plotting tools to explore and understand Synthea data. 
-- MAGIC 
-- MAGIC Reference for [Spark SQL built-in functions](https://spark.apache.org/docs/latest/api/sql/)
-- MAGIC 
-- MAGIC **Note** Some questions in this section are marked as _Extra Credit_. If you can skip or if you got extra time, try writing query to answer those. The correct queries are not unique, but you can find some suggested queries _extra_credit.sql_ file; feel free to check them if you are interested.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Database Structure
-- MAGIC The first step in data analytics is understanding the data. Using SQL queries we can get a sense of the data organization (database, tables, attributes) and common values. 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC -- this should fail if you haven't selected the right database
-- MAGIC 
-- MAGIC select * from encounters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Review all the database in your DBFS environment, and find the one we have uploaded on previous notebook:

-- COMMAND ----------

-- hidden on html-version due to privacy
show databases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Print all the tables in emr_sample database: 

-- COMMAND ----------

use emr_sample;

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Take a peek at the tables you are curious about:

-- COMMAND ----------

select * from encounters limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Or check their schema description:

-- COMMAND ----------

desc encounters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sometimes it is easier to switch to Python to explore the data content. We can simply run spark sql query & convert the result to pandas dataframe and play with it:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # print all the tables & describe them
-- MAGIC tables = spark.sql("show tables").toPandas()
-- MAGIC synthea_tables = tables[tables.database == 'emr_sample']['tableName'].values
-- MAGIC 
-- MAGIC for syntab in synthea_tables:
-- MAGIC   print(f'{syntab}')
-- MAGIC   print(spark.sql(f'describe table {syntab}').toPandas())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You rarely need to get all the rows from patient table and download it! If the data is too large, the UI only shows the first 1000 records:

-- COMMAND ----------

select * from patients

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit_: What is the total number of patients?

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Stat Check

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check specialities and see if the numbers match what you expect in reality:

-- COMMAND ----------

select speciality, count(*) tally 
from providers 
group by speciality

-- COMMAND ----------

-- DBTITLE 1,Encounters
select * from encounters

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC _Extra Credit:_ What was the date of the most recent encounter for each patient?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Freq. & Cardinalities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC One "code" could map to multiple descriptions:

-- COMMAND ----------

select count(*) tally, code, collect_set(description) description_list 
from observations 
group by code 
order by size(description_list) desc

-- COMMAND ----------

select count(*) tally, code, collect_set(description) description_list 
from conditions 
group by code 
order by size(description_list) desc


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

select * from conditions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Calculate patients' ages at the condition start:

-- COMMAND ----------

-- DBTITLE 1,Age at onset
select
  p.first,
  p.last,
  floor(
    datediff(date(c.start), date(p.birthdate)) / 365.24
  ) age_at_onset,
  c.description condition_description
from
  conditions c, patients p
where c.patient = p.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see what are the codes for the breast-cancer related conditions:

-- COMMAND ----------

select
  c.description,
  count(*)
from
  conditions c
where
  lower(c.description) rlike('breast')
group by
  c.description 
  
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

select
  p.gender,
  cast(o.value as float) T_score
from
  observations o
  join patients p on o.patient = p.id
where
  o.description == 'DXA [T-score] Bone density'

-- COMMAND ----------

-- DBTITLE 1,Medications
select
  *
from
  medications
limit
  10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Plots in R
-- MAGIC Plots assist us in investigating the data and getting quick insight and understanding. 
-- MAGIC 
-- MAGIC One can switch to R to use its powerful packages for data wrangling and plotting; e.g., dplyr, ggplot2 and sparklyr.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC options(repr.plot.width=600, repr.plot.height=1200)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Age-gender distribution for different conditions.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC library(dplyr)
-- MAGIC library(sparklyr)
-- MAGIC library(ggplot2)
-- MAGIC 
-- MAGIC sc <- spark_connect(method = "databricks")
-- MAGIC 
-- MAGIC conditions <- c(
-- MAGIC       'Dislocation of hip joint (disorder)', 
-- MAGIC       'Closed fracture of hip', 
-- MAGIC       'Osteoarthritis of hip', 
-- MAGIC       'Malignant neoplasm of breast (disorder)')
-- MAGIC 
-- MAGIC sql <- sprintf(
-- MAGIC       "select 
-- MAGIC             p.first, 
-- MAGIC             p.last, 
-- MAGIC             p.gender, 
-- MAGIC             c.description condition,
-- MAGIC             floor(datediff(date(c.start), date(p.birthdate))/365.24) age_at_onset 
-- MAGIC       from conditions c join patients p
-- MAGIC       where  c.patient = p.id
-- MAGIC       and c.description in ('%s')", 
-- MAGIC       paste(conditions, collapse="','")
-- MAGIC       )
-- MAGIC 
-- MAGIC 
-- MAGIC sdf_sql(sc, sql) %>% ggplot(aes(x=age_at_onset, fill=gender)) + geom_density(alpha=0.5) + facet_grid(condition ~ ., scales='free_y')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A finer-grain look reveals some issues or questionable observations in the data:

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC sdf_sql(sc, sql) %>% 
-- MAGIC   ggplot(aes(x=age_at_onset, fill=gender)) + geom_histogram(binwidth = 1) + facet_grid(condition ~ gender, scales='free_y')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A normal distribution is what we expect in real population, not bi-modal one.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC 
-- MAGIC library(dplyr)
-- MAGIC library(sparklyr)
-- MAGIC library(ggplot2)
-- MAGIC 
-- MAGIC sc <- spark_connect(method = "databricks")
-- MAGIC sql <- "
-- MAGIC     select 
-- MAGIC         description, 
-- MAGIC         cast(value as float) T_score 
-- MAGIC     from observations 
-- MAGIC     where description == 'DXA [T-score] Bone density'"
-- MAGIC 
-- MAGIC bone_density <- sdf_sql(sc, sql)
-- MAGIC bone_density %>% ggplot(aes(x=T_score)) + geom_density(fill='blue', alpha=0.5)

-- COMMAND ----------

-- select max(cast(value as float)) max_T_score from observations where description == 'DXA [T-score] Bone density'
select
  value T_score,
  count(*) tally
from
  observations
where
  description == 'DXA [T-score] Bone density'
group by
  value
order by
  cast(value as float)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC sql <- "
-- MAGIC     select 
-- MAGIC         p.gender, 
-- MAGIC         cast(o.value as float) T_score 
-- MAGIC     from observations o, patients p
-- MAGIC     where
-- MAGIC         o.patient = p.id 
-- MAGIC         AND o.description == 'DXA [T-score] Bone density'"
-- MAGIC sdf_sql(sc, sql) %>% ggplot(aes(x=T_score, fill=gender)) + geom_density(alpha=0.5)

-- COMMAND ----------

-- select * from encounters limit 10
select
  code,
  collect_set(description),
  count(*) tally
from
  encounters
group by
  code
order by
  tally desc 
  
-- encounterclass
-- 'General examination of patient (procedure)', "Encounter for 'check-up'"
-- 162673000   General examination of patient (procedure)
-- 185349003   Encounter for check up (procedure)

-- COMMAND ----------

-- select count(*) from conditions where code = 254837009 -- 1369
with encounter_condition as (
  select
    e.id encounter,
    e.code encounter_code,
    collect_set(c.code) as condition_code_list
  from
    encounters e
    join conditions c on c.encounter = e.id 
    -- where e.code in ('162673000', '185349003')
  group by
    e.id,
    e.code
),
encounter_bcadx as (
  select
    encounter,
    encounter_code,
    case
      when array_contains(condition_code_list, '254837009') then 'Y'
      else 'N'
    end as breast_cancer_dx
  from
    encounter_condition
)
select
  *
from
  encounter_bcadx
where
  breast_cancer_dx = 'Y' 
  
-- select breast_cancer_dx, count(*) tally from encounter_bcadx group by breast_cancer_dx

-- COMMAND ----------


-- select count(*) from conditions where code = 254837009 -- 1369
-- code 254837009 'Malignant neoplasm of breast (disorder)'
 
with
encounter_condition as (
  select e.id encounter, e.code encounter_code, collect_set(c.code) as condition_code_list
    from encounters e join conditions c on c.encounter = e.id
    group by e.id, e.code
),
encounter_bcadx as (
  select encounter, encounter_code, case when array_contains(condition_code_list, '254837009') then 'Y' else 'N' end as breast_cancer_dx 
    from encounter_condition
)
select * from encounter_bcadx where breast_cancer_dx = 'Y'
-- select breast_cancer_dx, count(*) tally from encounter_bcadx group by breast_cancer_dx
