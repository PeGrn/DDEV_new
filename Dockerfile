FROM bitnami/spark:3.3.1

USER root

# Installer les dépendances supplémentaires
RUN apt-get update && \
    apt-get install -y python3-pip wget && \
    apt-get clean

# Créer les répertoires nécessaires pour les JARs
RUN mkdir -p /opt/airflow/dags/jars

# Télécharger les JARs nécessaires
RUN wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -P /opt/airflow/dags/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar -P /opt/airflow/dags/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar -P /opt/airflow/dags/jars/

# Copier les JARs dans le répertoire de Spark
RUN cp /opt/airflow/dags/jars/*.jar /opt/bitnami/spark/jars/

# Installer airflow et ses dépendances
RUN pip install apache-airflow==2.7.1 boto3 requests

# Définir les variables d'environnement pour Airflow
ENV AIRFLOW_HOME=/opt/airflow