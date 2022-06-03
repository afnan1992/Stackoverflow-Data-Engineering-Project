from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.analytics import Spark
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.aggregator import Fluentd
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.network import Nginx
from diagrams.onprem.queue import Kafka
from diagrams.generic.storage import Storage
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.container import Docker
from diagrams.programming.language import Python

with Diagram(name="Stackoverflow Data Engineering project", show=False):
    
    
    with Cluster("Docker Container"):
        with Cluster("Airflow"):
            container = [ Docker(),Airflow()]
        with Cluster("Workflow"):
            stackoverflow = Python("Stackoverflow api")
            storage = Storage("csv files")
            container >> stackoverflow
            with Cluster("DB Cluster"):
                db_primary = PostgreSQL("staging")
                db_primary - PostgreSQL("datawarehouse")

    stackoverflow >> storage >> db_primary

     
