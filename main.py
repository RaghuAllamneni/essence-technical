import write_csv_to_bigquery as main


GCP_AUTH_TOKEN='sigma-freedom-280010-070ed71b8545.json'
GCP_BUCKET_NAME='essence_dev'
MAPPING_FILE_PATH='input/Mapping_file.csv'
MOAT_FILE_PATH='input/Moat_weekly_share.csv'
OP_MOAT_FILE_NAME='output/updatedWeeklyShare.csv'
OP_GCS_FILE_PATH='output/updatedWeeklyShare.csv'
GCP_PROJECT='sigma-freedom-280010'
GCP_BQ_DATA_SET='essence_dev'
GCP_BQ_TARGET_TABLE='weekly_moat_data'

if __name__ == '__main__':
    main.load_weekly_moat_data_into_bigquery(gcp_auth_token=GCP_AUTH_TOKEN
                                           , gcp_bucket_name=GCP_BUCKET_NAME
                                           , mapping_file_path=MAPPING_FILE_PATH
                                           , moat_file_path=MOAT_FILE_PATH
                                           , op_moat_file_name=OP_MOAT_FILE_NAME
                                           , op_gcs_file_path=OP_GCS_FILE_PATH
                                           , gcp_project=GCP_PROJECT
                                           , gcp_bq_data_set=GCP_BQ_DATA_SET
                                           , gcp_bq_target_table=GCP_BQ_TARGET_TABLE)