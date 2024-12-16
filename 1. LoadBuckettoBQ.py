# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# custom Python logic for deriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(
        dag_id = 'load_raw_data',
        catchup = False,
        schedule_interval = timedelta(days=1),
        default_args = default_args
        )    as dag:
    
# Dummy start task
    start =  DummyOperator(
        task_id = 'start',
        dag = dag,
        )
    
# GCS to Bigquery data load Operator and task
    t1 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'us_country',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['us_county.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.us_country',
        schema_fields = [
                            {'name':'date', 'type': 'date', 'mode': 'NULLABLE'},
                            {'name':'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'state', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'fips', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'cases', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deaths', 'type': 'INT64', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t2 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'us_states',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['us_states.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.us_states',
        schema_fields = [
                            {'name':'date', 'type': 'date', 'mode': 'NULLABLE'},
                            {'name':'state', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'fips', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'cases', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deaths', 'type': 'INT64', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t3 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'states_abbreviation',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['states_abv.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.states_abv',
        schema_fields = [
                            {'name':'state', 'type': 'STRING', 'mode':'NULLABLE'},
                            {'name':'abbreviation', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t4 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'country_population',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['County_Population.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.country_population',
        schema_fields = [
                            {'name':'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'Id2', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'state', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'population_est_2018', 'type': 'INT64', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t5 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'country_codeqs',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['CountryCodeQS.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.country_codeqs',
        schema_fields = [
                            {'name':'country', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'alpha_2_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'alpha_3_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'numeric_code', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                            {'name':'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t6 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'us_daily',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['us_daily.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.us_daily',
        schema_fields = [
                            {'name':'date', 'type': 'STRING', 'mode':'NULLABLE'},
                            {'name':'states', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positive', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'pending', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'inICUCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'inICUCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'onVentilatorCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'onVentilatorCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'dateChecked', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'death', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalized', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestResults', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'lastModified', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'recovered', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'total', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'posNeg', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deathIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestResultIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hash', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t7 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'states_daily',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['states_daily.csv'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.states_daily',
        schema_fields = [
                            {'name':'date', 'type': 'STRING', 'mode':'NULLABLE'},
                            {'name':'states', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'positive', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'probableCases', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'pending', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestResultSource', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'totalTestResults', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'inICUCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'inICUCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'onVentilatorCurrently', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'onVentilatorCumulative', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'recovered', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name': 'lastUpdateEt', 'type': 'STRING','mode': 'NULLABLE'},
                            {'name': 'dateModified', 'type': 'STRING','mode': 'NULLABLE'},
                            {'name': 'checkTimeEt', 'type': 'STRING','mode': 'NULLABLE'},
                            {'name':'death', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalized', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedDischarged', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'dateChecked', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'totalTestsViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveTestsViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeTestsViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveCasesViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deathConfirmed', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deathprobable', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestEncountersViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestsPeopleViral', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestsAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveTestsAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeTestsAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestsPeopleAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveTestsPeopleAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeTestsPeopleAntibody', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestsPeopleAntigen', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveTestsPeopleAntigen', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestsAntigen', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveTestsAntigen', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'fips', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'total', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'totalTestResultIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'posNeg', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'dataQualityGrade', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'deathIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hospitalizedIncrease', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'hash', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'commercialScore', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeRegularScore', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'negativeScore', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'positiveScore', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'score', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'grade', 'type': 'INT64', 'mode': 'NULLABLE'}
                        ],
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

    t8 = GoogleCloudStorageToBigQueryOperator(
        task_id = 'usa_hospital_beds',
        gcp_conn_id = 'GCP_Gouri',
        bucket = 'covid_data_bucket_24',
        source_objects = ['usa_hospital_beds.json'],
        destination_project_dataset_table = 'covid-data-analysis-443010.covid_pre_model.usa_hospital_beds',
        schema_fields = [
                            {'name':'OBJECTID', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'HOSPITAL_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'HOSPITAL_TYPE', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'HQ_ADDRESS', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'HQ_CITY', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'HQ_STATE', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'HQ_ZIP_CODE', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'COUNTY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'STATE_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name':'STATE_FIPS', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'CNTY_FIPS', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'FIPS', 'type': 'INT64', 'mode': 'NULLABLE'},                            
                            {'name':'NUM_LICENSED_BEDS', 'type': 'INT64', 'mode': 'NULLABLE'},                            
                            {'name':'NUM_STAFFED_BEDS', 'type': 'INT64', 'mode': 'NULLABLE'},                            
                            {'name':'NUM_ICU_BEDS', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'ADULT_ICU_BEDS', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'PEDI_ICU_BEDS', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                            {'name':'BED_UTILIZATION', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                            {'name':'AVG_VENTILATOR_USAGE', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                            {'name':'Potential_Increase_In_Bed_Capac', 'type': 'INT64', 'mode': 'NULLABLE'},
                            {'name':'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                            {'name':'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'}                         
                        ],
        source_format='NEWLINE_DELIMITED_JSON',
        skip_leading_rows = 1,
        create_disposition = 'CREATE_IF_NEEDED',
        write_disposition = 'WRITE_TRUNCATE',
    dag = dag
    )

#Dummy end task
    end = DummyOperator(
        task_id = 'end',
        dag = dag,
    )

#setting up task dependency
start >> [t1,t2,t3,t4,t5,t6,t7,t8] >> end
    