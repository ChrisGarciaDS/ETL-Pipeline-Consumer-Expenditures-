# ETL-Pipeline-Consumer-Expenditures-

Team 3: John Chen, Sau Chow, Christopher Garcia


### Overview:

The purpose of this project is to present to code a basic ELT pipeline implementation using Python & SQL. The purpose of this pipeline is to extract Consumer Expenditures data as well as relevant economic indicators data (ie GDP and CPI) and preprocess/clean it and then house it in a "Business Warehouse" where it is ready to be used and consumed by end users such as data scientists, data analysts, executives who need dashboards to make business decisions, in our case maybe an economists or researcher. This application uses various python libraries including SQLAlchemy and Pandas. The 'Staging Area' and 'Business Warehouse' utilizes a MySQL database locally installed on a personal laptop.

### How to Deploy Pipeline:

Run the team3-etl-pipeline.py file in your python development environment.

### How to Monitor Pipeline:

Check the log messages in etl-pipeline.log.  Log messages will be written to this file as the application executes.
