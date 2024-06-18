from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

class SeoulApiDateColumnSensor(BaseSensorOperator):
    template_fields = ('endpoint','check_date')

    def __init__(self,dataset_nm,check_date,**kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm +'/1/5'
        self.check_date = check_date

    def poke(self,context):
        import requests
        import json
        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{self.endpoint}/'
        self.log.info(f'url:{url}')
        self.log.info(f"{self.check_date}")
        response = requests.get(url)
        contents = json.loads(response.text)
        self.log.info(f"response: {contents}")
        if self.find_check_date_in_json(contents, self.check_date):
            self.log.info('데이터 갱신 확인')
            return True
        else:
            self.log.info('데이터 미갱신')
            return False

    def find_check_date_in_json(self,data, check_date):
        if isinstance(data, dict):
            for key, value in data.items():
                if self.find_check_date_in_json(value, check_date):
                    return True
        elif isinstance(data, list):
            for item in data:
                if self.find_check_date_in_json(item, check_date):
                    return True
        elif isinstance(data, str):
            if check_date in data:
                return True
        return False