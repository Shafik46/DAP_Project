In order to successfully execute the pipeline following steps need to be done to create an environment.

1. docker-compose.yaml file contains all the configuration to airflow and postgres
2. Create following directory 
	mkdir -p ./dags ./logs ./plugins ./code ./chart
3. Run following commands in terminal to setup airflow through dockers.
	"docker compose up airflow-init"
	"docker compose up -d"
4. Run "Docker ps" and it will show the if the containers are running
5. http://localhost:8080/ ---> Airflow will be up and running here.
	Username - airflow
	passwd   - airflow
6. Login to the postgres server with following credentials

	  host="localhost",
        port=5432,
        user="airflow",
        password="airflow"
7. Create Database DAP_Project using following commands.
	"CREATE DATABASE DAP_Project;"
	"CREATE SCHEMA Electric_Vehicles;"

8. Create tables by executing the DML in code/sql folders with name EV_Population_DML.sql and EV_Registration_DML.sql

9. EV_registration data is huge to pull through api. download that file and drop it in code folder in order to picked up by airflow 
https://catalog.data.gov/dataset/electric-vehicle-title-and-registration-activity

10. Now trigger the pipeline (make sure the dag file is in dag folder, .py file in code folder as mounted on the docker or else airflow won't recognise)
	
