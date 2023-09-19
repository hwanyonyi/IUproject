# Project Name
Belgian Freight Transport Data Engineering Project:

The project aims to preprocess and analyze On-Board Unit (OBU) data from Belgian lorries with a Maximum Authorized Mass exceeding 3.5 tonnes. The goal is to predict traffic conditions on a network scale using GPS data, including position, speed, and direction. The owners of lorries exceeding 3.5 tonnes must pay a kilometer charge for road usage. With over 140,000 trucks daily, at 2.00 am on a daily basis, each OBU device on the trucks produces data points containing a unique identifier, a timestamp, GPS position, engine speed and direction. This project focuses on data that was produced at 15 minute intervals for the perios January to  March 2019.



## Table of Contents

- [Project Name](#project-name)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
  - [Usage](#usage)
  - [Contributing](#contributing)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)

## Introduction

Data Processing:
The focus of this project shall be to collect this data, store the raw data in hive data lake. The following transformations will be applied to the data before it is delivered to the warehouse:
Standardize the velocity field to two decimal places, split by day of the week to form two tables, one for weekdays and the other for weekends while transforming the data from .CSV to json file format. The json file format is chosen to allow for future schema evolution. The datetime field shall be converted to UTC before it is delivered to the apache spark data warehouse. The data analysts and machine learning engineers can then take over the processing from the warehouse

## Features

This project has the following features

- Pipeline orchestration service - Airflow
- API Gateway service - Django
- Database services for airflow - postgres
- Hadoop Services - the datalake
    - name node
    - datanode
    - hive-metastore
    - hive-server
    - hive-webhcat
    - hue 
- Spark Services
    - spark-master
    - spark-worker
    - livy
The project incorporates a comprehensive set of features to effectively manage and analyze data related to Belgian lorry traffic. Airflow, the pipeline orchestration service, automates workflows and ensures timely data processing. Django serves as the API gateway for secure data access, while PostgreSQL provides a robust database backend for Airflow. The Hadoop ecosystem, including HDFS, Hive, and Hue, forms the data lake, facilitating the storage and retrieval of large datasets. Spark services, such as Spark Master, Spark Worker, and Livy, enable distributed data processing, ensuring scalability and efficient analysis. These components collectively create a robust infrastructure for processing and analyzing lorry traffic data, enhancing data-driven decision-making and insights.

## Getting Started
clone the repository using the link https://github.com/hwanyonyi/IUproject.git 

### Prerequisites

Docker desktop, Code editor preferrably Visual Studio Code. All other preriquisites will be installed automatically once you start the installation. The project also assumes user has intermediate knowledge of docker and python

### Installation

After cloning the repository to your local computer into a folder that is similarly named, proceed with the following step by step instructions to reproduce the data pipeline

Preliminary:
- Ensure Docker Desktop is installed and running on your local computer.
- Navigate to the project's root folder.
- Execute the command './start.sh' to initiate the setup process.
- Note that the first-time setup may take around 30 minutes, and it's recommended to allocate at least 4 CPU cores.
- This command creates all the necessary microservices for the project.
- After setup, check container statuses using 'docker ps'; all should be healthy except the gateway container.
- Airflow serves as the orchestration and monitoring tool for the pipeline.
- Access Airflow via 'http://localhost:8080.'
- Log in with the credentials: username 'airflow' and password 'airflow.'


**Task 1:CHECK IF THE API IS AVAILABLE**
On the airflow main menu: go to Admin and click on Connections to add a new connection
1.	Click on the plus sign:
a.	Conn Id: transport_api
b.	Conn Type: HTTP
c.	Host: http://localhost:8000/
2.	Click on save. This is all you need to get the connection working
Testing the connection:
1.	In your terminal, navigate to the project folder: run the command docker ps 
2.	Obtain the airflow container id then log into the container interactively: 
docker exec -it 0b7f07451d53 /bin/bash
3.	Test the first task of our pipeline. This will confirm that the API is available. Run the following command from inside the airflow container:
airflow@0b7f07451d53:/$ airflow tasks test transport_data_pipeline is_transport_data_available 2023-01-01
This should give you a success notification like similar to the one bellow

**Task 2:CHECK IF THE TRANSPORT FILE IS AVAILABLE**
1.	Go back to airflow interface web user interface: click on Admin then select connections
2.	Click the plus sign to add a new connection and configure only the following options
a.	conn id: transport_path 
b.	connection type : File(path)
c.	Extra: {"path":"/opt/airflow/dags/files"}

3.	Run the following commands in your terminal
$ docker ps : this will allow you to get the container ID
$ docker exec –it (container id)  /bin/bash
Run the following command from inside the airflow container:
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline is_transport_data_file_available 2023-01-01
You should get a success message

**Task 3:DOWNLOAD THE TRANSPORT DATA FILE, SPLIT THE DATA INTO WEEKED AND WEEKDAY AND CONVERT THE DATA INTO JSON**
This task involves the execution of the airflow python operator
Test this task by running the following command in your terminal: remember to use the correct container ID
$ docker ps : this will allow you to get the container ID
$ docker exec –it (container id)  /bin/bash

-bash-
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline downloading_transport_data 2023-01-01

For some reason, this command may throw this error: 
TypeError: split_csv_by_weekday_weekend() missing 3 required positional arguments: 'input_csv_file', 'output_weekday_json', and 'output_weekend_json'

But the files will be created automatically allowing us to proceed to the next task test



**Task 4: SAVING THE TRANSPORT FILES INTO HADOOP DISTRIBUTED FILE SYSTEM (HDFS)**

- Utilize the airflow bash operator to transfer the JSON transport files into HDFS.
- Access Hue at: http://localhost:32762/ using the credentials username: 'root' and password: 'root.'
- Hue provides the capability to query and explore HDFS data, tables, and databases.
- Upon logging in to Hue, close the welcome and tutorial pop-up windows to proceed.
- Verify the absence of any existing tables or data in hue.

Execute the following commands in your terminal: remember to have the correct airflow container ID
$ docker ps : this will allow you to get the container ID
$ docker exec –it (container id)  /bin/bash

-bash-
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline saving_weekday 2023-01-01

You should get a success message similar to this:
INFO - Marking task as SUCCESS. dag_id=transport_data_pipeline, task_id=saving_weekday, execution_date=20230101T000000, start_date=20230915T204624, end_date=20230915T204651

Apply the same process to save the weekend table to HDFS.
Run the following command from within the airflow container:
-bash-
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline saving_weekend 2023-01-01

Success message:
INFO - Marking task as SUCCESS. dag_id=transport_data_pipeline, task_id=saving_weekend, execution_date=20230101T000000, start_date=20230915T205006, end_date=20230915T205018 

Go back to your browser and access the Hue web interface. This time round you should be able navigate and find two folders created under the file browser: weekday and weekend

Certainly, here's the content in point/bullet format:

**Task 5: Creating a Hive Table to Overlay Your Files**

- Hive serves as our Data Lake, enabling us to execute SQL-like queries on our data.
- Return to the Airflow webpage and create a new connection, following the previous procedure.
- Pay attention to these parameters, as they are the only ones to be updated for this task:
  - Conn Id: hive_conn
  - Conn Type: Hive Server 2 Thrift
  - Login: hive
  - Password: hive
  - Port: 10000
- Once you've added the specified parameters, click "Save" to create the connection.
- Go back to the Hue webpage and navigate to the default database.
- Refresh the page to verify that no tables exist yet.

Go back to your terminal and run the following commands to test the creation of the Hive tables that will overlay the data
$ docker ps : this will allow you to get the container ID
$ docker exec –it (container id)  /bin/bash

-bash-
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline creating_weekday_table 2023-01-01

Success message: 
INFO - Marking task as SUCCESS. dag_id=transport_data_pipeline, task_id=creating_weekday_table, execution_date=20230101T000000, start_date=20230915T212609, end_date=20230915T212636

Repeat the above procedure for the weekend table:

-bash-
airflow@f39186273514:/$ airflow tasks test transport_data_pipeline creating_weekend_table 2023-01-01

Success message:
INFO - Marking task as SUCCESS. dag_id=transport_data_pipeline, task_id=creating_weekend_table, execution_date=20230101T000000, start_date=20230915T213052, end_date=20230915T213108

Go back to Hue web interface and verify that the tables have been created by refreshing the page:

Now you should see two table named weekday_traffic and weekend_traffic 

Certainly, here's the content in point format suitable for a README.md file:

**Task 6: Submitting Data to Our Spark Warehouse**

- Data processing will occur in SPARK, and online analytical processing (OLAP) will be conducted from this location. No data processing will be performed from Airflow.
- Two scripts will be executed in this process, one for weekday data and another for weekend data.
- The script will transform the date field to UTC time format as it submits the data to the Spark Warehouse, allowing data analysts and machine-learning engineers to easily query the tables and understand the data.
- In the Airflow web browser, create a new connection as previously done. Update only the following parameters:
  - Conn Id: spark_conn
  - Conn Type: Spark
  - Host: spark://spark-master
  - Port: 7077
- Click "Save" and proceed to test the task in your code editor following the procedure used in previous tasks.
- From within the Airflow container, enter and run the following command in your code editor:
  ```
  airflow@f39186273514:/$ airflow tasks test transport_data_pipeline weekday_processing 2023-01-01
  ```
  You should receive a success notification.
- Repeat the same Spark Submit job test for the weekend data by running the following Airflow command:
  ```
  airflow@f39186273514:/$ airflow tasks test transport_data_pipeline weekend_processing 2023-01-01
  ```
  You should get a success notification after the test has completed running.
- Access the Hue web user interface and run the same HQL query as before. You should see some data.


Certainly, here's the content in point format for a README.md file:

**Task 7: Send Email Notification Task**

- This task allows you to receive notifications when the execution status of your pipeline is complete. In our case, we will receive an email when our DAG is done.
- To configure email notifications, you need to set up your email provider to send emails from your data pipeline using your email address. In our example, we use Gmail. You will need to generate an application-specific password from your email provider, which you'll pass to the email operator in Airflow to send emails from your address.
- Follow these steps:
  1. Copy and paste this link into your web browser: https://security.google.com/settings/security/apppasswords
  2. Sign in when prompted.
  3. Select "App," in this case, choose "Mail."
  4. Select the device, then click "Generate."
  5. This generates a password; copy and save it securely, then click "Done."
- In the project repository, navigate to the folder `mnt > airflow > airflow.cfg`.
- `airflow.cfg` is the configuration file for Airflow. Edit the SMTP settings to include your email address and the generated password.
- Save your changes to the configuration file.
- In your terminal, restart your Airflow container with the following command:
  ```
  docker-compose restart airflow
  ```
- Now you can add the "Send Email" task to your DAG and configure the task accordingly. In our example, we send the email to a Yahoo account, but the email address can be for any messaging provider.
- Test your task by executing the following commands:
  ```
  docker ps # to get the container ID
  docker exec –it (container id) /bin/bash
  airflow@f39186273514:/$ airflow tasks test transport_data_pipeline send_email_notification 2023-01-01
  ```
- You should receive a success notification, and you'll be able to receive an email when your DAG completes.
