# ETL_EXCHANGE_RATES
Orchestrate an ETL where we schedule the following tasks:
- Check if API is availbale
- Download the exchange rates data from the API
- Saving the downloaded data in HDFS
- Exposing the saved data in a table through Hive
- Processing this data with Spark
- Sending an email to expose the success of the ETL


### Initialise all the instances
``` bash
./start.sh
```

### Check that all the containers are healthy
``` bash
docker ps
```

### Set-up all the needed connections for the different tasks in the Airflow UI
- {"Conn Id": "exchange_rates_api", "Conn Type": "HTTP", "Host": "https://api.apilayer.com/"}
- {"Conn Id": "hive_conn", "Conn Type": "Hive Server 2 Thrift", "Host": "hive-server", 
    "Login": "hive", "Password": "hive", "Port": 10000}
- {"Conn Id": "spark_conn", "Conn Type": "Spark", "Host": "spark://spark-master", "Port": 7077}

### Go to http://localhost:32762/ and create an account "root" with password "root"

### Get into the container of airflow. Where 8f670f456eab is the container_id_of_airflow
``` bash
docker exec -it 8f670f456eab /bin/bash
```

### Test the different tasks from the DAG
``` bash
airflow tasks test exchange_rates_data_pipeline is_exchange_rates_available 2019-01-01
airflow tasks test exchange_rates_data_pipeline downloading_exchange_rates 2019-01-01
airflow tasks test exchange_rates_data_pipeline saving_exchange_rates 2019-01-01
airflow tasks test exchange_rates_data_pipeline creating_exchange_rates_table 2019-01-01
airflow tasks test exchange_rates_data_pipeline exchange_rates_processing 2019-01-01
airflow tasks test exchange_rates_data_pipeline send_email_notification 2019-01-01
```

### If all the tasks are marked as SUCCESS, then your DAG is read to be activated through the
### Airflow UI at http://localhost:8080/
