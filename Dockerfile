# Official Airflow image 
FROM apache/airflow:2.9.0-python3.9 

# Set the working directory in the container
WORKDIR /opt/expdir

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies from requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current/local directory into the container
COPY . .

# Set the AIRFLOW_HOME 
ENV AIRFLOW_HOME=/opt/expdir/airflow

# Airflow directories
RUN mkdir -p /opt/expdir/airflow/dags /opt/expdir/airflow/logs /opt/expdir/airflow/plugins

# For testing purposes
# CMD ["tail", "-f", "/dev/null"]