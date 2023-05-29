# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # New Capability Overview: Data Lineage with Unity Catalog
# MAGIC 
# MAGIC Databricks Unity Catalog, the unified governance solution for all data and AI assets on Lakehouse, brings support for data lineage. Data lineage includes capturing all the relevant metadata and events associated with the data in its lifecycle, including the source of the data set, what other data sets were used to create it, who created it and when, what transformations were performed, what other data sets leverage it, and many other events and attributes. With a data lineage solution, data teams get an end-to-end view of how data is transformed and how it flows across their data estate. **Lineage is supported for all languages (SQL, Python, Scala and R)** and is captured down to the column level. Lineage data includes notebooks, workflows, and dashboards related to the query. Lineage can be visualized in Data Explorer in near real-time and retrieved with the Databricks REST API.
# MAGIC 
# MAGIC In this demo we are going to demonstrate how to capture and view lineage data at table and column level for upstream and downstream artifacts.
# MAGIC 
# MAGIC **Learning Objectives**
# MAGIC 
# MAGIC By the end of this demo, you should be able to;
# MAGIC 
# MAGIC * Describe how data lineage works on Databricks
# MAGIC 
# MAGIC * Capture data lineage at table and column level
# MAGIC 
# MAGIC * View lineage graphs for tables and columns
# MAGIC 
# MAGIC * Describe data lineage permission model

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Requirements
# MAGIC 
# MAGIC * Tables must be registered in a Unity Catalog metastore to be eligible for lineage capture
# MAGIC 
# MAGIC * Lineage is computed on a 30 day rolling window
# MAGIC 
# MAGIC * To view lineage, users must have necessary permissions; SELECT privilege on the table, and view permission for the notebook, workflow or dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Enable Data Lineage For A Cluster
# MAGIC 
# MAGIC **📌 Note:** With the general availability of data lineage, all workloads referencing the Unity Catalog metastore now have **data lineage enabled by default**. This means all workloads reading or writing to Unity Catalog will automatically capture lineage. **To take advantage of automatically captured Data Lineage, please restart any clusters or SQL Warehouses that were started prior to December 7th, 2022.** 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC The first thing we're going to do is to run a setup script. This script will define the required configuration variables that are scoped to each user.
# MAGIC 
# MAGIC This script will create a catalog and a schema that we are going to use in this demo. In additon, custom functions that will create and populate sample tables are defined in this file.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Other Conventions
# MAGIC 
# MAGIC Run code block below to view necessary details that we will need in this course. Note the **catalog name** and schema name that we are going to use to create tables and later inspect the lineage graphs.
# MAGIC 
# MAGIC In addition, as you progress through this course, you will see various references to the object **`DA`**. This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricsk API. For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here bellow. In this course we are going to use UI mostly, therefore, we are not going to need them for now.

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"User Catalog Name: {DA.catalog_name}")
print(f"Schema Name:       default")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Data for a lineage scenario
# MAGIC 
# MAGIC In order to keep the course as simple as possible, we are not going to use any dataset. Instead, we will create and fill tables manually. This will allow us to define relationship between tables and columns in a table. 
# MAGIC 
# MAGIC In this demo, we are going to create 3 main tables;
# MAGIC - `customers`
# MAGIC - `products`
# MAGIC - `orders` 
# MAGIC 
# MAGIC Next, we are going to join these tables to create `customer_daily_spending` table, which will include daily total spending per customer. 
# MAGIC 
# MAGIC And finally, we are going to use this table to create `customer_yearly_spending`. This table `concat` columns to demonstrate how column level lineage works. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage
# MAGIC 
# MAGIC Data lineage with UC supports **table level** and **column level** data lineage. First, we are going to demonstrate how lineage graph looks like at table level and then show how it works at column level.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Table Level Data Lineage
# MAGIC 
# MAGIC Let's first start by creating 3 main tables and inspect the lineage tab. 
# MAGIC 
# MAGIC Run code block below to create the `customers`, `products` and `order` tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Tables (Customer, Products, Orders)

# COMMAND ----------

# Create Customers, Products and Orders tables

create_customers_table()

create_products_table()

create_orders_table()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Inspect Lineage Graph - Indepedent Tables
# MAGIC 
# MAGIC For now, we don't have any relationship between tables. Let's inspect the lineage graph for each table and check the ****notebook tab**** to see the ****Upstream**** notebook. For all 3 tables, you should see the name of this notebook as this is the notebook that creates tables.
# MAGIC 
# MAGIC To view the lineage graph;
# MAGIC 
# MAGIC - Click **Data** on the left panel to go to **Data Explorer** page.
# MAGIC 
# MAGIC - From the left panel, select the **Catalog** that is created for you in this course. The catalog name starts with your user name.
# MAGIC 
# MAGIC - Select the **default** schema and select a table that you want to view the lineage data.
# MAGIC 
# MAGIC - In the schema page, select **Lineage** tab.
# MAGIC 
# MAGIC - Click **Select Lineage Graph** button to view the lineage graph for the selected table.
# MAGIC 
# MAGIC - To view upstream and downstream entities in the lineage logs;
# MAGIC     
# MAGIC     - Select entity type from the left tab list. You can view Tables, Notebooks, Workflows and Dashboards.
# MAGIC     
# MAGIC     - You can view **Upstream** and **Downstream** entities by selecting entity type on the top.
# MAGIC 
# MAGIC - There is a refresh button that can be used to refresh lineage data.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Tables (Daily and Yearly Expenses)
# MAGIC 
# MAGIC The lineage graph for indepent tables isn't very interesting. In real-life scenarios, we usually have complex relationships between different data entities. Data lineage becomes more important as the relationship network becomes more complicated. 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Let's create a daily spending table for customers which will join data from three tables that we just created. Then, using this table, we will create another table that will show yearly spend for customers.
# MAGIC 
# MAGIC We are not going to have a very complex structure in this demo, but at least we can demonstrated how lineage helps you to track how data flows. 

# COMMAND ----------

# Create daily and yearly expense tables for customers
create_customer_daily_spending_table()

create_customer_yearly_spending_table()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Inspect Lineage Graph - Relationship
# MAGIC 
# MAGIC Running the code block above will create two new tables. 
# MAGIC 
# MAGIC - **Customer_daily_spending** table: This table is created by joining the three tables that we created in the previous step. Also, its data is used by the **customer_yearly_spend** table. This means, in Tables tab, **Upstream** section, you should see that these three tables are listed. On the **Downstream** section, you will see **customer_yearly_spend** table.
# MAGIC 
# MAGIC - **Customer_yearly_spending**: This table's **Upstream** is the **customer_daily_spending** table. In addition, this table has a `description` field that is created by concating three fields. 
# MAGIC 
# MAGIC Follow the same steps as described in the previous step. **Go to Data Explorer → Select Catalog and Schema → Select Table → Select Lineage**. 
# MAGIC 
# MAGIC Inspect lineage graph for both daily and yearly customer spend tables. You should be able to see the **Upstream** and **Downstream** tables for each table. In addition, you should be able to see the notebook name that triggered the lineage process.  

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Inspect Column Level Lineage Graph
# MAGIC 
# MAGIC In addition to the table level lineage logging, UC captures column level lineage data. This allows us to inspect the where a table column is used 
# MAGIC 
# MAGIC To view column-level lineage;
# MAGIC 
# MAGIC - Click **Data** on the left panel to go to **Data Explorer** page.
# MAGIC 
# MAGIC - From the left panel, select the **Catalog** that is created for you in this course. The catalog name starts with your user name.
# MAGIC 
# MAGIC - Select the **default** schema and select a table that you want to view the lineage data.
# MAGIC 
# MAGIC - Click the **Schema** tab.
# MAGIC 
# MAGIC - Select the column that you want to view lineage data.
# MAGIC 
# MAGIC - This will open a side-panel that you can view the **Upstream** and **Downstream** resources.
# MAGIC 
# MAGIC Following these steps,  view the columns of `customer_yearly_spending` table and inspect the column lineage for each column.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lineage Permission Model
# MAGIC 
# MAGIC Lineage graphs in Unity Catalog are privilege-aware and share the same permission model as Unity Catalog. If users do not have access to a table, they will not be able to explore the lineage associated with the table, adding an additional layer of security for privacy considerations.
# MAGIC 
# MAGIC If a user does not have the SELECT privilege on the table, they will not be able to explore the lineage.
# MAGIC 
# MAGIC To demonstrate this, let's share some of the tables that we just created a *test-user* while leaving other as they are. If we re-inspect the lineage graphs for the tables, we should be able to see the lineage data only for the tables that are shared with the user. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Integration with 3rd-part tools
# MAGIC 
# MAGIC Lineage data can be easily exported via the REST API. We are not going to cover the REST API in this course. You can <a href="https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#data-lineage-api" target="_blank">read more about the REST API on this page</a>.
# MAGIC 
# MAGIC In this course we inspected the visualized lineage data in the Data Explorer in near real-time. With REST API, lienage data can be retrived via REST API to support integrations with Databricks' catalog partners.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Deleting Lineage Data
# MAGIC 
# MAGIC **The default lineage data retention period is 30 days.** Which means if an artifact doesn't use a table or a column, it will be deleted from the lineage graph within 30 days.
# MAGIC 
# MAGIC 
# MAGIC If you want to delete lineage data to meet compliance requirements or any other requirement, **you must delete the metastore managing the Unity Catalog objects**. This will delete all objects stored in Unity Catalog! In this course we are not going to demonstrate this process but you can check <a href="https://docs.databricks.com/data-governance/unity-catalog/create-metastore.html#delete" target="_blank">this page</a> for more details. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean up Classroom
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC 
# MAGIC Databricks Unity Catalog let you track data lineage out of the box.No extra setup required, just read and write from your table and the engine will build the dependencies for you. Lineage can work at a table level but also at the column level, which provide a powerful tool to track dependencies on sensible data. Lineage can also show you the potential impact of updating a table/column and find who will be impacted downstream.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>