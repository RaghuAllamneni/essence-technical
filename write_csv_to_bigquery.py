from sys import prefix
from typing import IO
import numpy as np
from google.cloud import storage
import pandas as pd
import io
from io import BytesIO
from google.cloud import bigquery


def load_weekly_moat_data_into_bigquery(gcp_auth_token, gcp_bucket_name, mapping_file_path, moat_file_path, op_moat_file_name, op_file_path, gcp_project, gcp_bq_data_set, gcp_bq_target_table):
    print('Initialising connection to the gcs bucket..... \n')
    storage_client = storage.Client.from_service_account_json(gcp_auth_token)
    bucket=storage_client.get_bucket(gcp_bucket_name)

    
    print('Reading mapping file from the gcs bucket..... \n')
    mappingBlob=bucket.blob(mapping_file_path)
    mappingData=mappingBlob.download_as_string()
    mappingDf=pd.read_csv(io.BytesIO(mappingData), encoding='utf-8',sep=',')

    print('Reading weekly moat file from the gcs bucket..... \n')
    moatBlob=bucket.blob(moat_file_path)
    moatData=moatBlob.download_as_string()
    moatDf=pd.read_csv(io.BytesIO(moatData), encoding='latin-1',sep=',')

    print('Joining the weekly moat file and mapping file..... \n')
    joinDf = pd.merge(moatDf, mappingDf, how='left', left_on='third_column_label', right_on='third_column')

    print('Updating the fourth_column_label in weekly moat file..... \n')
    joinDf['fourth_column_label']=np.select([pd.notna(joinDf['OPID_string_Updated'])] ,[joinDf['OPID_string_Updated']], default=joinDf['fourth_column_label'])

    print('Retaining only the columns related to moat file..... \n')
    colsToRetain = joinDf.columns[:-3]

    print('Writing the updated moat data into a csv file..... \n')
    joinDf.to_csv(op_moat_file_name, mode='w', columns=colsToRetain, index = False)

    print('Pushing the updated moat file to the GCS bucket..... \n')
    blob=bucket.blob(op_file_path)
    blob.upload_from_filename(op_moat_file_name)

    # Construct a BigQuery client object.
    print('Constructing a Bigquery client object..... \n')
    client = bigquery.Client.from_service_account_json(gcp_auth_token)

    print('Declaring job_config to load the data from csv into Bigquery table..... \n')    
    job_config = bigquery.LoadJobConfig(
        schema=[
        bigquery.SchemaField("brand_id", "STRING"),
        bigquery.SchemaField("Dataset_Type", "STRING"),
        bigquery.SchemaField("Country", "STRING"),
        bigquery.SchemaField("Dataset_Name", "STRING"),
        bigquery.SchemaField("first_column", "STRING"),
        bigquery.SchemaField("first_column_label", "STRING"),
        bigquery.SchemaField("second_column", "STRING"),
        bigquery.SchemaField("second_column_label", "STRING"),
        bigquery.SchemaField("third_column_x", "STRING"),
        bigquery.SchemaField("third_column_label", "STRING"),
        bigquery.SchemaField("fourth_column_x", "STRING"),
        bigquery.SchemaField("fourth_column_label", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("Impressions_Analyzed_unfiltered", "STRING"),
        bigquery.SchemaField("Impressions_Analyzed", "STRING"),
        bigquery.SchemaField("Valid_Impressions", "STRING"),
        bigquery.SchemaField("two_Sec_In_View_Impressions", "STRING"),
        bigquery.SchemaField("five_Sec_In_View_Impressions", "STRING"),
        bigquery.SchemaField("Valid_and_Viewable_Impressions", "STRING"),
        bigquery.SchemaField("Valid_and_Fully_On_Screen_Measurable_Impressions", "STRING"),
        bigquery.SchemaField("In_ViewTime_above_5_Sec_Impressions", "STRING"),
        bigquery.SchemaField("Reached_1st_Quartile_Sum", "STRING"),
        bigquery.SchemaField("Reached_2nd_Quartile_Sum", "STRING"),
        bigquery.SchemaField("Reached_3rd_Quartile_Sum", "STRING"),
        bigquery.SchemaField("Reached_Complete_Sum", "STRING"),
        bigquery.SchemaField("Audible_On_Complete_Sum", "STRING"),
        bigquery.SchemaField("Visible_On_Completion_Sum", "STRING"),
        bigquery.SchemaField("Audible_and_Visible_on_Complete_Sum", "STRING"),
        bigquery.SchemaField("Audible_and_Fully_On_Screen_for_Half_of_Duration_Impressions", "STRING"),
        bigquery.SchemaField("Moat_Video_Score", "STRING")
            ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        )
    uri = 'gs://'+gcp_bucket_name+'/'+op_file_path

    table_id=gcp_project+'.'+gcp_bq_data_set+'.'+gcp_bq_target_table

    print('Loading the data from the updated moat csv file into Bigquery table..... \n')
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    print('Getting the table properties from Bigquery..... \n')
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))