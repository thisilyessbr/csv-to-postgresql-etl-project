@echo off
set PYSPARK_PYTHON=%cd%\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%cd%\venv\Scripts\python.exe
%cd%\venv\Scripts\python.exe test_spark_setup.py