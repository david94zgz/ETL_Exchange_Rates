Get the docker-compose.yaml

Create the logs, dags and plugins folders

Import some variables 
``` bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

Initialise the Airflow instance 
``` bash
docker-compose up airflow-init
docker-compose up
```

Check if the containers are up and running
``` bash
docker ps
```

Access to the Airflow command line interface. Where 44f7d2f8f884 is the container ID
``` bash
docker exec 44f7d2f8f884
```

Check if a task is correctly coded. Where exchange_rates_data_pipeline is the name of the DAG,
is_exchange_rates_available is the name of the task and 2021-01-01 is a date in the past
``` bash
docker exec -it 44f7d2f8f884 /bin/bash
airflow tasks test exchange_rates_data_pipeline is_exchange_rates_available 2021-01-01
```
