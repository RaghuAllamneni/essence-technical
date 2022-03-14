# essence-technical
# Introduction

This project is split into two usecases

- Join two csv files and update one of the column in one of the csv file and load the updated csv file into a Bigquery table
- SQL scripts to populate the datamart to be used by downstream teams

## Use case - 1:
### Solution:
In this use case the csv files are assumed to be placed in the gcs bucket
python pandas is used to read the csv files into dataframes
Once the data is read into dataframes the two dataframes are left joined based on the key fields as advised
Fourth column label in moat weekly share file is updated wherever there is a match between the third column label and third column in the mapping file
The updated data is written into a csv file
The output csv file is loaded into a Bigquery table via a gcs bucket

### Assumptions:
The solution is developed with an assumption that both the input csv files (Mapping_file.csv and Moat_weekly_share.csv) will be placed inside a gcs bucket

### Scope of improvements:
The above solution is implemented with 9 Parameters, making it to flexible to be convereted as a generic framework (Still need to be make some chnages to make it as a generic framework, based on the use cases)
The csv schemas and the join keys can be stored in a config file to make the code a generic framework
Bigquery table schema can also be stored in the same config file, so that the framework can take the csv file names, Bigquery target table name as input parameter and can be ran for any combination of csv files
Only one positive unit test case is written for now, but can be added more test cases

## Use case-2:
* The sql scripts are written as create view statements as it will be easier to be passed in to the downstram teams
* The budget pace metrics data is saved as a csv file to compare the data with the refrennce data provided
