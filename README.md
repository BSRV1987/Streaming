# Streaming
A sample Stream processing pipeline

# Environment That is recommended

GitHub
Gitpod
VisualstudioCode

# Files and their functionality

1. Prod-env1.py --> This is part of the streaming pipeline does flatten job only. It transforms the unstructured JSON format to a tabular form and save the data as csv file.  

2. Prod-env2.py --> This job aggregates the values of the columns every 5 seconds by taking the sum of the columns from the files that arrived in the last 5 seconds in delta mode. This has been altered to 5 seconds instead of 5 minutes. With 5 minutes, spark job is writing empty information into parquet files as stream is maintaining intermediate state and have to wait for 5 mins to view the data.

3. main.py --> this is to generate source files on a continuous basis. Press ctrl+c to cancel the execution

# Requirements

Have pyspark library installed on the environment using pip

pip install pyspark


# execution instructions

Have 3 seperate Terminals for executing 3 python files.

Commands Terminal 1 :

CD POC
CD SourceFiles
python main.py

Commands Terminal 2 :

CD POC
python Prod-env1.py


Commands Terminal 3 :

CD POC
python Prod-env2.py
