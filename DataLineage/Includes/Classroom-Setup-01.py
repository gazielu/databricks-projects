# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student

spark.sql(f"USE CATALOG {DA.catalog_name};")        # Note: you can replace {DA.catalog_name} with another catalog name if you want to use a different catalog

# COMMAND ----------

def create_customers_table():
    spark.sql(f"CREATE OR REPLACE TABLE customers (id INT, name STRING, city STRING);")
    spark.sql(
        f"""INSERT INTO customers VALUES 
        (1001, "Shirley Smith", "BREMEN"), 
        (1002, "Carmen Guzman", "VIENNA"), 
        (1003, "Diana Hentz", "COLUMBUS"), 
        (1004, "Marco Tirado", "Otselic"), 
        (1005, "Mark Uczen", "Roseland"), 
        (1006, "James Linton", "City of Sonoma"), 
        (1007, "Jeffrey Malik", "PITTSBURGH"), 
        (1008, "Ursula Izewski", "Washington County"), 
        (1009, "Ricardo Mendez", "Riverhead"),
        (1010, "Fatina Hardin", "Bedford")
    """
    )
    print("Customers table is created and filled with sample data.")

# COMMAND ----------

def create_products_table():
    spark.sql(f"CREATE OR REPLACE TABLE products (id INT, name STRING, category STRING);")
    spark.sql(
        f"""INSERT INTO products VALUES
        (1001, "Phone", "Electronics"),
        (1002, "T-shirt", "Clothes"),
        (1003, "Bread", "Bakery"),
        (1004, "Lemonade", "Beverage"),
        (1005, "Water", "Beverage"),
        (1006, "TV", "Electronics"),
        (1007, "Yoghurt", "Food"),
        (1008, "Banana", "Produce"),
        (1009, "Shampoo", "Personal Care"),
        (1010, "Headphone", "Electronics");
    """
    )
    print("Products table is created and filled with sample data.")

# COMMAND ----------

def create_orders_table():
    spark.sql(
        f"CREATE OR REPLACE TABLE orders (id INT, amount INT, customer_id INT, product_id INT, date DATE);"
    )
    spark.sql(
        f"""INSERT INTO orders VALUES
        (1001, 800, 1001, 1004, DATE '2022-02-15'),
        (1002, 24, 1005, 1003, DATE '2022-04-01'),
        (1003, 1200, 1010, 1006, DATE '2021-09-13'),
        (1004, 15, 1001, 1007, DATE '2022-01-26'),
        (1005, 30, 1007, 1002, DATE '2021-11-17'),
        (1006, 18, 1003, 1009, DATE '2022-05-14'),
        (1007, 75, 1006, 1010, DATE '2021-10-29'),
        (1008, 8, 1006, 1008, DATE '2022-03-21'),
        (1009, 5, 1006, 1005, DATE '2022-02-05'),
        (1010, 546, 1002, 1001, DATE '2022-04-02');
    """
    )
    print("Orders table is created and filled with sample data.")

# COMMAND ----------

def create_customer_daily_spending_table():
    spark.sql(
        f"""CREATE OR REPLACE TABLE customer_daily_spending AS
            SELECT
                c.name,
                SUM(amount) AS total_expense,
                category,
                date
            FROM
                orders AS o
                JOIN products AS p ON o.product_id = p.id
                JOIN customers AS c ON o.customer_id = c.id
            GROUP BY
                c.name,
                category,
                date
        """
    )
    print("Customer daily spending table is created based on Customers, Orders and Products tables")

# COMMAND ----------

def create_customer_yearly_spending_table():
    spark.sql(
        f"""CREATE OR REPLACE TABLE customer_yearly_spending AS
            SELECT 
                *,
                concat(name,"'s total expense in ", year, " was $", total_expense) AS description
            FROM(
                SELECT
                    name,
                    date_format(date, 'y') AS year,
                    SUM(total_expense) AS total_expense
                FROM customer_daily_spending
                GROUP BY name, year
            )
        """
    )
    print("Customer yearly spending table is created based on daily spending table.")
