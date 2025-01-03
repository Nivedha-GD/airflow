from airflow.sensors.filesystem import FileSensor
class CustomFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')
