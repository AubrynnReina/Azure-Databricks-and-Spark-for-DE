-- Databricks notebook source
drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed
location 'abfss://processed@aubdbcourse.dfs.core.windows.net/';

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_presentation
location 'abfss://presentation@aubdbcourse.dfs.core.windows.net/';
