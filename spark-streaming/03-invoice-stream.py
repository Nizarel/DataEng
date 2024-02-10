# Databricks notebook source
dbutils.widgets.text("rimo", "xx")
proz = dbutils.widgets.get("rimo")
print(proz)

# COMMAND ----------

dbutils.widgets.combobox
print(dbutils.widgets.get("foo"))

# COMMAND ----------

class invoiceStream():
    def __init__(self):
        self.basee_data_dire = ""
    
    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string, CustomerType string
