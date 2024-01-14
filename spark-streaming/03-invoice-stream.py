# Databricks notebook source
class invoiceStream():
    def __init__(self):
        self.basee_data_dire = ""
    
    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string, CustomerType string
