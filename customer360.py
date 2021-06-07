from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import settings
from airflow.models import Connection

dag = DAG(
	dag_id = 'customer_360_pipeline_p_5',
	start_date=days_ago(1)
)

sensor = HttpSensor(
	task_id='watch_for_orders',
	http_conn_id='order_s3',
	endpoint='orders.csv',
	response_check=lambda response: response.status_code == 200,
	dag=dag,
	retry_delay=timedelta(minutes=5),
	retries=12
)

def get_order_url():
	session = settings.Session()
	connection = session.query(Connection).filter(Connection.conn_id == 'order_s3').first()
	return f'{connection.schema}://{connection.host}/orders.csv'

download_order_cmd = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'


download_to_edgenode = SSHOperator(
	task_id='download_orders',
	ssh_conn_id='cloudera_conn',
	command=download_order_cmd,
	dag=dag
)

#no incremental load and no partitioning
def fetch_customer_info_cmd():
	command_one = "hive -e 'DROP TABLE customers_4'"
	command_one_extn = "hadoop fs -rm -R /user/root/customers"
	command_two = "sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username retail_dba --password cloudera --table customers --hive-import --create-hive-table --hive-table customers_4"
	return f'({command_one} || {command_one_extn}) && {command_two}'
	#return command_two


import_customer_info = SSHOperator(
	task_id='download_customers',
	ssh_conn_id='cloudera_conn',
	command=fetch_customer_info_cmd(),
	dag=dag
)

upload_orders_info = SSHOperator(
	task_id='upload_orders_to_hdfs',
	ssh_conn_id='cloudera_conn',
	command='hadoop fs -rm -R -f /user/cloudera/airflow_input && hadoop fs -mkdir -p /user/cloudera/airflow_input && hadoop fs -put /root/airflow_pipeline/orders.csv /user/cloudera/airflow/input/',
	dag=dag
)


def get_order_filter_cmd():
	command_one = 'hadoop fs -rm -R -f /user/cloudera/airflow/output/'
	command_two = 'spark-submit --class ProcessCompletedOrders /home/cloudera/Desktop/week20.jar'
	return f'{command_one} && {command_two}'


process_order_info = SSHOperator(
	task_id='process_orders',
	ssh_conn_id='cloudera_conn',
	command=get_order_filter_cmd(),
	dag=dag
)



def create_order_hive_table_cmd():
	return 'hive -e "CREATE external table if not exists airflow.orders(order_id int, order_date string, customer_id int, status string) row format delimited fields terminated by \',\' stored as textfile \'/user/cloudera/final_output_nithesh\' " '

 
create_order_table = SSHOperator(
	task_id='create_orders_table_hive',
	ssh_conn_id='cloudera_conn',
	command=create_order_hive_table_cmd(),
	dag=dag
)


def load_hbase_cmd():
	command_one = 'hive -e "create table if not exists  airflow.airflow_hbase(customer_id int, customer_fname string, customer_lname string, order_id int, order_date string) STORED BY \'org.apache.hadoop.hive.hbase.HBaseStorageHandler\' with SERDEPROPERTIES(\'hbase.columns.mapping\'=\':key,personal:customer_fname, personal:customer_lname, personal:order_id,personal:order_date\')"'
	command_two = 'hive -e "insert overwrite table airflow.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname,o.order_id, o.order_date from airflow.customers c join airflow.orders o ON (c.customer = o.customer)"' 



load_hbase = SSHOperator(
	task_id='load_hbase_tables',
	ssh_conn_id='cloudera_conn',
	command=load_hbase_cmd(),
	dag=dag
)


dummy = DummyOperator(
	task_id='dummy',
	dag = dag
)

#sensor >> import_customer_info >> dummy
#sensor >> download_to_edgenode >> upload_orders_info >> process_order_info >> create_order_table >> dummy



sensor >> import_customer_info
sensor >> download_to_edgenode >> upload_orders_info >> process_order_info >> create_order_table 
[import_customer_info, create_order_table] >> load_hbase > dummy