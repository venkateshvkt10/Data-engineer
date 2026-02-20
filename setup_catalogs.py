# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE SCHEMA if  not EXISTS fmcg.bronze;
# MAGIC CREATE SCHEMA if  not EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA if  not EXISTS fmcg.gold;