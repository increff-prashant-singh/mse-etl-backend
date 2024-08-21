import os
import logging
import traceback
from flask import Blueprint, jsonify, request
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask_cors import CORS
import mysql.connector  # or import pymysql if you're using PyMySQL
from mysql.connector import Error  # or use pymysql
from flask import g  # global context for database connection
from flask import current_app,redirect
from werkzeug.exceptions import HTTPException 

load_dotenv()

views = Blueprint('views', __name__)
CORS(views,supports_credentials=True)



# Function to get a database connection
def get_db_connection():
    if 'db' not in g:
        g.db = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DB')
        )
    return g.db

class ApiException(Exception):
    def __init__(self, status_code, message):
        super().__init__(self)
        self.status_code = status_code
        self.message = message

@views.route('/getStatus/<factory>/<pipeline>', methods=['GET'])
def getStatus(factory, pipeline):
    try:
        logging.info(f"Parameters received: factory={factory}, pipeline={pipeline}")

        client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        )

        resource_group_name = "rg-ms-etl-prod"

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)

        filter_parameters = {
            'lastUpdatedAfter': start_time.isoformat() + 'Z',
            'lastUpdatedBefore': end_time.isoformat() + 'Z',
            'filters': [
                {
                    'operand': 'PipelineName',
                    'operator': 'Equals',
                    'values': [pipeline]
                }
            ]
        }

        pipeline_runs = client.pipeline_runs.query_by_factory(
            resource_group_name=resource_group_name,
            factory_name=factory,
            filter_parameters=filter_parameters
        )

        if pipeline_runs.value:
            latest_run = pipeline_runs.value[-1]
            run_id = latest_run.run_id

            pipeline_run_status = client.pipeline_runs.get(
                resource_group_name=resource_group_name,
                factory_name=factory,
                run_id=run_id
            )

            response_dict = pipeline_run_status.as_dict()
            logging.info(f"Pipeline status: {response_dict.get('status')}")
            return jsonify(response_dict)

        else:
            return jsonify({"error": f"No pipeline runs found for pipeline '{pipeline}' within the specified time range."}), 404

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@views.route('/createRun/<factory>/<pipeline>', methods=['POST'])
def createRun(factory, pipeline):
    try:
        parameters = request.json if request.json else {}

        client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        )

        response = client.pipelines.create_run(
            resource_group_name="rg-ms-etl-prod",
            factory_name=factory,
            pipeline_name=pipeline,
            parameters=parameters
        )

        return jsonify(response.as_dict()), 200

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@views.route('/cancelRun/<factory>/<pipeline_name>', methods=['POST'])
def cancelRun(factory, pipeline_name):
    try:
        # Initialize Data Factory Management Client
        client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        )

        # Define the time window to search for recent runs (e.g., last 24 hours)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)

        # Get the latest pipeline run
        pipeline_runs = client.pipeline_runs.query_by_factory(
            resource_group_name="rg-ms-etl-prod",
            factory_name=factory,
            filter_parameters={
                "lastUpdatedAfter": start_time,
                "lastUpdatedBefore": end_time,
                "filters": [
                    {"operand": "PipelineName", "operator": "Equals", "values": [pipeline_name]}
                ]
            }
        )

        # Check if there are any pipeline runs
        if not pipeline_runs.value:
            return jsonify({"error": f"No runs found for pipeline {pipeline_name} in the last 24 hours"}), 404

        # Assuming the latest run is the first one (sorted by LastUpdated)
        latest_run = max(pipeline_runs.value, key=lambda run: run.last_updated)
        latest_run_id = latest_run.run_id

        # Cancel the latest run
        client.pipeline_runs.cancel(
            resource_group_name="rg-ms-etl-prod",
            factory_name=factory,
            run_id=latest_run_id,
        )

        return jsonify({"message": f"Run {latest_run_id} for pipeline {pipeline_name} cancelled successfully"}), 200

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

