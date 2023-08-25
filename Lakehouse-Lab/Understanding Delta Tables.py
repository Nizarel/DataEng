# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employees
# MAGIC --using Delta
# MAGIC   (id INT, name STRING, salary DOUBLE);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (1,"Adam", 3500.0),
# MAGIC   (2, "Sarah", 4025.5),
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3),
# MAGIC   (5, "Anna", 2500.0),
# MAGIC   (6, "Kim", 6200.3)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail employees

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/user/hive/warehouse/employees'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Update employees
# MAGIC set salary = salary + 100
# MAGIC where name like "A%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history emloyees
