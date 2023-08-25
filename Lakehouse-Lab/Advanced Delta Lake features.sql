-- Databricks notebook source
DESCRIBE HISTORY employees

-- COMMAND ----------

DROP TABLE emloyees

-- COMMAND ----------

select *
from employees version as of 1

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------


