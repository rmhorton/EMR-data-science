# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook uses the [vis.js](https://visjs.org/) Javascript language to create interactive visualizations of co-occurrence graphs.
# MAGIC 
# MAGIC The documentation for [Network](https://visjs.github.io/vis-network/docs/network/) structures describes the options available for [nodes](https://visjs.github.io/vis-network/docs/network/nodes.html) and [edges](https://visjs.github.io/vis-network/docs/network/edges.html).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Library of Functions

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import re
# MAGIC # import json
# MAGIC 
# MAGIC # import datetime
# MAGIC 
# MAGIC def get_nodes_and_edges_from_item_pair_stats(cooccurrence_pdf):
# MAGIC     item_stats = {r['item1']:{'count':r['item1_count'], 'prevalence':r['item1_prevalence']} 
# MAGIC                     for idx, r in cooccurrence_pdf.iterrows()}
# MAGIC 
# MAGIC     item_stats.update({r['item2']:{'count':r['item2_count'], 'prevalence':r['item2_prevalence']} 
# MAGIC                     for idx, r in cooccurrence_pdf.iterrows()})
# MAGIC 
# MAGIC     nodes_df = pd.DataFrame([{'label':k,'count':v['count'], 'prevalence':v['prevalence']}  
# MAGIC                     for k,v in item_stats.items()])
# MAGIC     nodes_df['id'] = nodes_df.index
# MAGIC    
# MAGIC     edges_df = cooccurrence_pdf.copy()
# MAGIC     node_id = {r['label']:r['id'] for idx, r in nodes_df.iterrows()}
# MAGIC     edges_df['from'] = [node_id[nn] for nn in edges_df['item1']]
# MAGIC     edges_df['to'] = [node_id[nn] for nn in edges_df['item2']]
# MAGIC     
# MAGIC     print("Your graph will have {0} nodes and {1} edges.".format( len(nodes_df), len(edges_df) ))
# MAGIC 
# MAGIC     return nodes_df, edges_df[[ 'from', 'to', 'both_count', 'confidence', 'lift']]
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC def export_to_vis_js(nodes_df, edges_df, title, html_file_name):
# MAGIC     """
# MAGIC     Generate vis_js graph from cooccurrence Pandas dataframe and write to HTML file.
# MAGIC     """
# MAGIC     default_metric = 'lift'
# MAGIC     max_lift = np.quantile(edges_df['lift'], 0.95)
# MAGIC     
# MAGIC     nodes_str = nodes_df.to_json(orient='records')
# MAGIC     edges_str = edges_df.to_json(orient='records')
# MAGIC     
# MAGIC     html_string = ( 
# MAGIC         '<!DOCTYPE html>\n'
# MAGIC         '<html lang="en">\n'
# MAGIC         '<head>\n'
# MAGIC         '	<meta http-equiv="content-type" content="text/html; charset=utf-8" />\n'
# MAGIC         f'	<title>{title}</title>\n'
# MAGIC         '	<script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>\n'
# MAGIC         f'	<script type="text/javascript">NODE_LIST={nodes_str};FULL_EDGE_LIST={edges_str};</script>\n'
# MAGIC         '	<style type="text/css">#mynetwork {width: 100%; height: 700px; border: 1px}</style>\n'
# MAGIC         '	</head>\n'
# MAGIC         '		<body>\n'
# MAGIC         '			<div class="slidercontainer">\n'
# MAGIC         '				<label>minimum edge strength:\n'
# MAGIC         '					<input type="range" min="0" max="1" value="0.5" step="0.01" class="slider" id="min_edge_weight">\n'
# MAGIC         '					<input type="text" id="min_edge_weight_display" size="2">\n'
# MAGIC         '					Metric: <span id="edge_weight_metric" />\n'
# MAGIC         '				</label>\n'
# MAGIC         '			</div>\n'
# MAGIC         '			<div id="mynetwork"></div>\n'
# MAGIC         '			<script type="text/javascript">\n'
# MAGIC         f'	const max_lift = {max_lift}\n'
# MAGIC         '	const urlParams = new URLSearchParams(window.location.search);\n'
# MAGIC         f'	const weight_metric = urlParams.get("metric")==null ? "{default_metric}" : urlParams.get("metric")\n'
# MAGIC         '	const sign_color = {pos:"blue", neg:"red", zero:"black"}\n'
# MAGIC         '	const filter_coef = {"confidence":1, "lift": max_lift, "log2lift": Math.log2(max_lift)}\n'
# MAGIC         '	\n'
# MAGIC         '	document.getElementById("min_edge_weight_display").value = filter_coef[weight_metric] * 0.5\n'
# MAGIC         '	document.getElementById("min_edge_weight").onchange = function(){\n'
# MAGIC         '		document.getElementById("min_edge_weight_display").value= filter_coef[weight_metric] * this.value\n'
# MAGIC         '	}\n'
# MAGIC         '	document.getElementById("edge_weight_metric").innerText = weight_metric\n'
# MAGIC         '	\n'
# MAGIC         '	EDGE_LIST = []\n'
# MAGIC         '	for (var i = 0; i < FULL_EDGE_LIST.length; i++) {\n'
# MAGIC         '		edge = FULL_EDGE_LIST[i]\n'
# MAGIC         '		edge["log2lift"] = Math.log2(edge["lift"])\n'
# MAGIC         '		edge["value"] = Math.abs(edge[weight_metric])\n'
# MAGIC         '		edge["title"] = weight_metric + " " + edge[weight_metric]\n'
# MAGIC         '		edge["sign"] = (edge[weight_metric] < 0) ? "neg" : "pos";\n'
# MAGIC         '		edge["color"] = {color: sign_color[edge["sign"]] };\n'
# MAGIC         '		if (weight_metric=="confidence"){ // both directions\n'
# MAGIC         '			edge["arrows"] = "to"\n'
# MAGIC         '			EDGE_LIST.push(edge)\n'
# MAGIC         '		} else { // one direction\n'
# MAGIC         '			if (edge["from"] < edge["to"]) EDGE_LIST.push(edge)\n'
# MAGIC         '		}\n'
# MAGIC         '	}\n'
# MAGIC         '	\n'
# MAGIC         '	const edgeFilterSlider = document.getElementById("min_edge_weight")\n'
# MAGIC         '	\n'
# MAGIC         '	function edgesFilter(edge){return edge.value >= edgeFilterSlider.value * filter_coef[weight_metric]}\n'
# MAGIC         '	\n'
# MAGIC         '	const nodes = new vis.DataSet(NODE_LIST)\n'
# MAGIC         '	const edges = new vis.DataSet(EDGE_LIST)\n'
# MAGIC         '	\n'
# MAGIC         '	const nodesView = new vis.DataView(nodes)\n'
# MAGIC         '	const edgesView = new vis.DataView(edges, { filter: edgesFilter })\n'
# MAGIC         '	\n'
# MAGIC         '	edgeFilterSlider.addEventListener("change", (e) => {edgesView.refresh()})\n'
# MAGIC         '	\n'
# MAGIC         '	const container = document.getElementById("mynetwork")\n'
# MAGIC         '	const options = {physics:{maxVelocity: 10, minVelocity: 0.5}}\n'
# MAGIC         '	const data = { nodes: nodesView, edges: edgesView }\n'
# MAGIC         '	new vis.Network(container, data, options)\n'
# MAGIC         '	\n'
# MAGIC         '			</script>\n'
# MAGIC         '		</body>\n'
# MAGIC         '	</html>\n'
# MAGIC     )
# MAGIC     with open(html_file_name, "wt") as html_file:
# MAGIC         html_file.write(html_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Compute item-pair statistics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We're going to name items by their descriptions, so we need to check that each item only has one description.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use emr_sample;
# MAGIC 
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Check for denormalized descriptions in medications
# MAGIC %sql
# MAGIC 
# MAGIC select code, collect_list(distinct lower(description)) description_list from medications group by code order by size(description_list) desc
# MAGIC 
# MAGIC -- medication code '999999' appears to be bogus; it is used for 4 different things. All the other differences in description are just in capitalization.

# COMMAND ----------

# DBTITLE 1,Check for denormalized descriptions in conditions
# MAGIC %sql
# MAGIC select code, count(*) tally, collect_list(distinct description) descriptions from conditions group by code order by size(descriptions) desc
# MAGIC 
# MAGIC --- only 4 codes have multiple descriptions; 3 of these are trivial differences
# MAGIC --- code '427089005' could be either "Male Infertility" or "Diabetes from Cystic Fibrosis"; we'll skip that code

# COMMAND ----------

# DBTITLE 1,Collect 'baskets' and 'items'
# MAGIC %sql
# MAGIC 
# MAGIC create or replace temporary view basket_item as
# MAGIC with
# MAGIC pe1 as (
# MAGIC   select enc.id encounter
# MAGIC       , floor(datediff(enc.start, pat.birthdate)/365.24) age
# MAGIC       , pat.race
# MAGIC       , pat.ethnicity
# MAGIC       , pat.gender
# MAGIC     from patients pat join encounters enc on enc.patient=pat.id
# MAGIC       where enc.encounterclass in ('inpatient', 'outpatient')
# MAGIC )
# MAGIC ,
# MAGIC pe2 as (
# MAGIC   select encounter, 
# MAGIC           concat_ws('_', 'gender', gender) gender, 
# MAGIC           concat_ws('_', 'ethnicity', ethnicity) ethnicity, 
# MAGIC           concat_ws('_', 'race', race) race, 
# MAGIC         case -- approximately 'MeSH' age ranges according to https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3825015/
# MAGIC           when age <  2 then 'age_00_01'
# MAGIC           when age <  5 then 'age_02_04'
# MAGIC 		  when age < 12 then 'age_05_11'
# MAGIC           when age < 18 then 'age_12_17'
# MAGIC           when age < 24 then 'age_18_23'
# MAGIC           when age < 44 then 'age_24_43'
# MAGIC           when age < 65 then 'age_44_64'
# MAGIC           when age < 80 then 'age_65_79'
# MAGIC           when age >=80 then 'age_80_plus'
# MAGIC           else 'age_unknown'
# MAGIC         end age_group
# MAGIC     from pe1
# MAGIC )
# MAGIC ,
# MAGIC code_tally as (
# MAGIC   select code, count(*) tally, first(description) description 
# MAGIC     from conditions 
# MAGIC     where code != '427089005' -- could be either "Male Infertility" or "Diabetes from Cystic Fibrosis"
# MAGIC     group by code
# MAGIC )
# MAGIC ,
# MAGIC encounter_condition_long as (
# MAGIC   select e.id encounter, ct.description condition
# MAGIC     from encounters e
# MAGIC     join conditions c on c.patient = e.patient
# MAGIC     join code_tally ct on ct.code = c.code
# MAGIC     join pe2 on e.id = pe2.encounter
# MAGIC     where ct.tally > 100
# MAGIC       and c.start < e.stop
# MAGIC       and (c.stop > e.stop or c.stop is null)
# MAGIC )
# MAGIC ,
# MAGIC bmi as (
# MAGIC   select encounter, value as bmi, 
# MAGIC         case  -- https://www.cdc.gov/healthyweight/assessing/bmi/adult_bmi/index.html
# MAGIC           when value < 18.5 then 'bmi_underweight'
# MAGIC           when value < 25   then 'bmi_healthy weight'
# MAGIC 		  when value < 30   then 'bmi_overweight'
# MAGIC           when value < 40   then 'bmi_obese'
# MAGIC           when value >= 40  then 'bmi_morbidly_obese'
# MAGIC           else 'bmi_unknown'
# MAGIC         end as bmi_category
# MAGIC     from observations where code = '39156-5'
# MAGIC )
# MAGIC ,
# MAGIC patient_features_long as (
# MAGIC   select encounter, stack(4, gender, ethnicity, race, age_group) as feature from pe2
# MAGIC )
# MAGIC select encounter as basket, concat('CONDITION:', condition) as item from encounter_condition_long
# MAGIC union
# MAGIC select encounter as basket, concat('PATIENT:', feature) as item from patient_features_long
# MAGIC union
# MAGIC select encounter as basket, concat('MEDICATION:', lower(description)) as item from medications where code != '999999'
# MAGIC union
# MAGIC select encounter as basket, concat('OBSERVATION:', bmi_category) as item from bmi
# MAGIC union
# MAGIC select encounter, concat('OBSERVATION:', value) from observations where description = 'Tobacco smoking status NHIS'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- select count(*) from basket_item; -- 25754861
# MAGIC 
# MAGIC select * from basket_item;

# COMMAND ----------

# DBTITLE 1,Calculate item-pair statistics
# MAGIC %sql
# MAGIC -- MIN_COUNT = 200
# MAGIC 
# MAGIC drop table if exists item_pair_stats;
# MAGIC 
# MAGIC create table item_pair_stats as
# MAGIC with 
# MAGIC   bi as (
# MAGIC     select basket, item
# MAGIC       from basket_item
# MAGIC       group by basket, item
# MAGIC   ),
# MAGIC   item_counts as (
# MAGIC     select item, count(*) item_count
# MAGIC       from bi
# MAGIC       group by item
# MAGIC   ),
# MAGIC   bi_count as (
# MAGIC     select bi.*, ic.item_count
# MAGIC       from bi
# MAGIC         join item_counts ic on bi.item=ic.item
# MAGIC       where ic.item_count > 200
# MAGIC   ),
# MAGIC   item_pair_stats as (
# MAGIC       select bi1.item item1, bi2.item item2,
# MAGIC               bi1.item_count item1_count, bi2.item_count item2_count,
# MAGIC               count(*) as both_count              
# MAGIC           from bi_count bi1
# MAGIC             join bi_count bi2
# MAGIC               on bi1.basket = bi2.basket and bi1.item != bi2.item
# MAGIC           group by bi1.item, bi1.item_count, 
# MAGIC                    bi2.item, bi2.item_count
# MAGIC   ),
# MAGIC   cc as (
# MAGIC     SELECT item1, item2, item1_count, item2_count, both_count,
# MAGIC           CAST(item1_count AS FLOAT)/(select count(distinct basket) from basket_item) as item1_prevalence,
# MAGIC           CAST(item2_count AS FLOAT)/(select count(distinct basket) from basket_item) as item2_prevalence,
# MAGIC           CAST(both_count AS FLOAT)/CAST(item1_count AS FLOAT) AS confidence
# MAGIC       FROM item_pair_stats
# MAGIC   )
# MAGIC select *, confidence/item2_prevalence lift from cc 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Explore item-pair statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from item_pair_stats order by confidence desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select item1, item2, confidence, lift from item_pair_stats 
# MAGIC   where item2 rlike 'Non-small cell lung cancer'
# MAGIC   and item1 rlike 'MEDICATION'
# MAGIC   order by lift desc;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Extra credit:
# MAGIC 
# MAGIC * How would you find the low-confidence examples?
# MAGIC 
# MAGIC * What medication has the highest lift for predicting Non-small cell lung cancer? Is it reasonable to use this as a predictor?
# MAGIC 
# MAGIC * What does a lift less than 1 mean?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Generate Interactive Co-occurrence Graph

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can't plot all the edges in this graph, so we need to filter out the weak ones. First let's plot a distribution and decide where to make the cut-off:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # select all the item pairs with confidence greater than 0.5
# MAGIC ip_stats = spark.sql("select * from item_pair_stats where confidence > 0.5").toPandas()
# MAGIC 
# MAGIC # reformat as two separate tables, one for nodes and the other for edges
# MAGIC nodes, edges = get_nodes_and_edges_from_item_pair_stats(ip_stats)
# MAGIC 
# MAGIC # decide which colors to use for the different categories of nodes
# MAGIC color_map = {'PATIENT': '#FF9999', 'CONDITION': '#9999FF', 'MEDICATION': '#99FF99', 'OBSERVATION':'#FFFF99'}
# MAGIC 
# MAGIC # split off the category type from the node label
# MAGIC label_parts = [lbl.split(':') for lbl in nodes['label']]
# MAGIC 
# MAGIC # make separate colums for node characteristics (to be used by the vis.js library)
# MAGIC nodes['category'] = [lp[0] for lp in label_parts]
# MAGIC 
# MAGIC # 'label' is the text that appears on the node
# MAGIC nodes['label'] = [lp[1] for lp in label_parts]
# MAGIC # yup, color
# MAGIC nodes['color'] = [color_map[cat] for cat in nodes['category']]
# MAGIC # 'title' is the text that appears on mouseover
# MAGIC nodes['title'] = [ '\n'.join([row['category'], 
# MAGIC                               row['label'], 
# MAGIC                               'count: ' + str(row['count']), 
# MAGIC                               'prevalence: ' + str(row['prevalence'])]) 
# MAGIC                   for i, row in nodes.iterrows()]
# MAGIC 
# MAGIC nodes

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(ip_stats.hist(column='confidence', bins=15)[0][0])  ### ???

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # make sure the plots directory exists, then save the cooccurrence plot there
# MAGIC dbutils.fs.mkdirs('/FileStore/plots')
# MAGIC export_to_vis_js(nodes, edges, 'Synthea Co-occurrence Demo', '/dbfs/FileStore/plots/synthea_cooccurrence_demo.html')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This is just a demo of linking to the file store. You will need to customize the hyperlink by copying the correct number from your own Databricks URL:
# MAGIC 
# MAGIC `https://adb-1953517438448055.15.azuredatabricks.net/?o=`__1953517438448055__`#notebook/3520119352938610/command/2583312897290483`
# MAGIC 
# MAGIC 
# MAGIC View results [here](https://adb-7320327251662587.7.azuredatabricks.net/files/plots/synthea_cooccurrence_demo.html?o=7320327251662587)

# COMMAND ----------


