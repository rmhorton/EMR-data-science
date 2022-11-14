# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Train an Explainable ML Classifier

# COMMAND ----------

# If you have not permanently installed this package on your cluster, you can just install it temporarily by removing the # sign from the next line:
#! pip install interpret

# COMMAND ----------

# MAGIC %sql
# MAGIC use emr_sample;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Breast Cancer

# COMMAND ----------

# MAGIC %sql
# MAGIC with
# MAGIC latest_patient_start as (
# MAGIC   select patient, max(date(start)) latest_start from encounters 
# MAGIC     group by patient
# MAGIC ),
# MAGIC most_recent_encounters as (
# MAGIC   select encounters.* 
# MAGIC     from encounters inner join latest_patient_start 
# MAGIC       on encounters.patient = latest_patient_start.patient 
# MAGIC       and date(encounters.start) = latest_patient_start.latest_start
# MAGIC     where date(encounters.start) > date('2021-01-01')
# MAGIC )
# MAGIC select e.id encounter, p.first, p.last, floor(datediff(date(e.start), date(p.birthdate))/365.24) age, p.race, p.ethnicity, p.gender
# MAGIC     , c.description
# MAGIC   from patients p
# MAGIC   inner join most_recent_encounters e on p.id = e.patient
# MAGIC     left join conditions c on c.patient = e.patient and date(c.start) <= date(e.stop)  where c.description = 'Malignant neoplasm of breast (disorder)'

# COMMAND ----------

# MAGIC %r
# MAGIC options(repr.plot.width=600, repr.plot.height=1200)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Breast Cancer
# MAGIC 
# MAGIC Predict whether breast cancer will be diagnosed in a given encounter. Exclude patients who have a current diagnosis of breast cancer.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select description, count(*) tally from conditions where code = '254837009' group by description

# COMMAND ----------

# DBTITLE 1,Feature engineering
# MAGIC %sql
# MAGIC create or replace temporary view patient_breast_cancer as
# MAGIC with 
# MAGIC retro_numbered_encounters as (
# MAGIC    SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY patient ORDER BY date(start) DESC) AS row_number
# MAGIC    FROM encounters
# MAGIC ),
# MAGIC most_recent_encounter as (
# MAGIC   select * from retro_numbered_encounters where row_number = 1
# MAGIC ),
# MAGIC breast_ca_conditions as (
# MAGIC   select * from conditions c where c.code = 254837009  -- 'Malignant neoplasm of breast (disorder)'
# MAGIC )
# MAGIC select concat_ws(' ', p.first, p.last) patient_name, p.gender, p.race, p.ethnicity,
# MAGIC         floor (datediff(date(e.start), date(p.birthdate))/365.24) age,
# MAGIC         case when c.code is null then 0 else 1 end as breast_cancer
# MAGIC   from most_recent_encounter e 
# MAGIC     join patients p on e.patient = p.id
# MAGIC     left outer join breast_ca_conditions c on c.patient = e.patient;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC select * from patient_breast_cancer limit 5;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pbc = spark.sql('select * from patient_breast_cancer').toPandas()
# MAGIC # type(pbc.age[0]) # decimal.Decimal
# MAGIC pbc['age'] = pbc['age'].astype(float)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pbc['age'].dtypes

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pandas as pd
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from interpret.glassbox import ExplainableBoostingClassifier
# MAGIC 
# MAGIC X = pbc[['gender', 'race', 'ethnicity', 'age']]
# MAGIC y = pbc['breast_cancer']
# MAGIC 
# MAGIC seed = 1
# MAGIC X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=seed)
# MAGIC 
# MAGIC ebm = ExplainableBoostingClassifier(random_state=seed)
# MAGIC ebm.fit(X_train, y_train)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from interpret import show
# MAGIC 
# MAGIC ebm_global = ebm.explain_global()
# MAGIC show(ebm_global)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC ebm_local = ebm.explain_local(X_test[10:15], y_test[10:15])
# MAGIC show(ebm_local)

# COMMAND ----------

# MAGIC %python
# MAGIC # y_test
# MAGIC 
# MAGIC p_test = ebm.predict_proba(X_test)[:,1]
# MAGIC 
# MAGIC actual_predicted_pdf = pd.DataFrame({'actual':y_test, 'predicted_probability':p_test})
# MAGIC 
# MAGIC ## I'll plot these densities in R. Export the data to the database:
# MAGIC spark.createDataFrame(actual_predicted_pdf).createOrReplaceTempView("actual_predicted")
# MAGIC 
# MAGIC ## or make a permanent table:
# MAGIC # actual_predicted_pdf.write.mode("overwrite").saveAsTable("actual_predicted")

# COMMAND ----------

# MAGIC %r
# MAGIC options(repr.plot.width=800, repr.plot.height=400)

# COMMAND ----------

# MAGIC %r
# MAGIC library(dplyr)
# MAGIC library(sparklyr)
# MAGIC library(ggplot2)
# MAGIC 
# MAGIC sc <- spark_connect(method = "databricks")
# MAGIC 
# MAGIC spark_read_table(sc, "actual_predicted") %>% 
# MAGIC   collect %>%  # download it locally
# MAGIC   mutate(actual=factor(actual)) %>%
# MAGIC   ggplot(aes(x=predicted_probability, fill=actual)) + geom_density(alpha=0.5)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from interpret import perf
# MAGIC roc = perf.ROC(ebm.predict_proba, feature_names=X_train.columns)
# MAGIC 
# MAGIC roc_explanation = roc.explain_perf(X_test, y_test)
# MAGIC show(roc_explanation)

# COMMAND ----------


