from sensors.seoul_api_date_column_sensor import SeoulApiDateColumnSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor2',
    start_date=pendulum.datetime(2024,6,14, tz='Asia/Seoul'),
    schedule='0 9 * * *',
    catchup=False
) as dag:
    sensor__tb_cycle_rent_use_day_info = SeoulApiDateColumnSensor(
        task_id='sensor__tb_cycle_rent_use_day_info',
        dataset_nm='ListAirQualityByDistrictService',
        check_date='{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash }}'
    )