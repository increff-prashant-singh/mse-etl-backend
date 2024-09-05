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

def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    # Other headers can be added here if needed
    return response

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

# @views.route('/cancelRun/<factory>/<pipeline_name>', methods=['POST'])
# def cancelRun(factory, pipeline_name):
#     try:
#         # Initialize Data Factory Management Client
#         client = DataFactoryManagementClient(
#             credential=DefaultAzureCredential(),
#             subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
#         )

#         # Define the time window to search for recent runs (e.g., last 24 hours)
#         end_time = datetime.utcnow()
#         start_time = end_time - timedelta(days=7)

#         # Get the latest pipeline run
#         pipeline_runs = client.pipeline_runs.query_by_factory(
#             resource_group_name="rg-ms-etl-prod",
#             factory_name=factory,
#             filter_parameters={
#                 "lastUpdatedAfter": start_time,
#                 "lastUpdatedBefore": end_time,
#                 "filters": [
#                     {"operand": "PipelineName", "operator": "Equals", "values": [pipeline_name]}
#                 ]
#             }
#         )

#         # Check if there are any pipeline runs
#         if not pipeline_runs.value:
#             return jsonify({"error": f"No runs found for pipeline {pipeline_name} in the last 24 hours"}), 404

#         # Assuming the latest run is the first one (sorted by LastUpdated)
#         latest_run = max(pipeline_runs.value, key=lambda run: run.last_updated)
#         latest_run_id = latest_run.run_id

#         # Cancel the latest run
#         client.pipeline_runs.cancel(
#             resource_group_name="rg-ms-etl-prod",
#             factory_name=factory,
#             run_id=latest_run_id,
#         )

#         #cancel the child pipeliens
#         child_runs = client.pipeline_runs.query_by_factory(
#             resource_group_name="rg-ms-etl-prod",
#             factory_name=factory,
#             filter_parameters={
#                 "lastUpdatedAfter": start_time,
#                 "lastUpdatedBefore": end_time,
#                 "filters": [
#                     {"operand": "TriggeredByName", "operator": "Equals", "values": [pipeline_name]}
#                 ]
#             }
#         )

#         # Cancel all identified child runs
#         for run in child_runs.value:
#             client.pipeline_runs.cancel(
#                 resource_group_name="rg-ms-etl-prod",
#                 factory_name=factory,
#                 run_id=run.run_id,
#             )


#         return jsonify({"message": f"Run {latest_run_id} for pipeline {pipeline_name} cancelled successfully"}), 200

#     except Exception as e:
#         logging.error(f"An error occurred: {str(e)}")
#         logging.error(traceback.format_exc())
#         return jsonify({"error": str(e)}), 500


@views.route('/cancelRun/<data_factory>/<pipeline_name>', methods=['POST'])
def CancelRecursively(data_factory,pipeline_name):
    latest_run = get_recent_pipeline_run_id(data_factory,pipeline_name)
    latest_run_dict = get_parentpipeline_dict(latest_run)
    if latest_run_dict['status']=='Success':
         return jsonify({"error": f"Pipeline {pipeline_name} is Successful So the run cannot be cancelled"}), 404
    elif latest_run_dict['status']=='Cancelled':
        return jsonify({"error": f"Pipeline {pipeline_name}  is already Cancelled'"}), 404
    else:
        cancelPipeline(latest_run_dict['run_id'],data_factory)
        pipelines_to_cancel = traverse_responses_recursively(latest_run_dict['run_id'],data_factory,latest_run_dict['time_parameters'])
        print(pipelines_to_cancel)
        return jsonify({"message": f"Run {latest_run_dict['run_id']} for pipeline {pipeline_name} cancelled successfully"}), 200                   
    return jsonify({"error": f"Run Ended'"}), 404     



def get_recent_pipeline_run_id(factory,pipeline_name):
    client = DataFactoryManagementClient(
    credential=DefaultAzureCredential(),        
    subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"))
        
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=7)
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

    if not pipeline_runs.value:
        return ({"error": f"No runs found for pipeline {pipeline_name} in the last 24 hours"}), 404

    latest_run = max(pipeline_runs.value, key=lambda run: run.last_updated)
    return latest_run


def converttime(timestr):
    time_obj = datetime.fromisoformat(timestr)
    iso_time_str = time_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return iso_time_str

def add_hour_to_isotime(timestamp):
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    new_dt = dt + timedelta(hours=1)
    new_timestamp = new_dt.isoformat().replace('+00:00', 'Z') 
    return new_timestamp


def get_pipeline_responses(run_id,time_params,data_factory):
    client = DataFactoryManagementClient(
        credential=DefaultAzureCredential(),        
        subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"))
    
    response = client.activity_runs.query_by_pipeline_run(
        resource_group_name="rg-ms-etl-prod",
        factory_name=data_factory,
        run_id=run_id,
        filter_parameters={
            "lastUpdatedAfter": time_params['start_time'],
            "lastUpdatedBefore": time_params['end_time'],
        },
    )
    return response


def get_parentpipeline_dict(latest_run):
    latest_run_dict = {
        'run_id'   : latest_run.run_id,
        'run_group_id' : latest_run.run_group_id,
        'is_Latest' : latest_run.is_latest,
        'pipeline_name' : latest_run.pipeline_name,
        'status': latest_run.status,
        'time_parameters' : {
        'start_time' : converttime(str(latest_run.run_start)),
        'end_time' : converttime(str(latest_run.last_updated))}
    }
    if latest_run_dict['status']=='InProgress':
        latest_run_dict['time_parameters']['end_time'] = add_hour_to_isotime(latest_run_dict['time_parameters']['end_time'])

    return latest_run_dict

def get_childpipelines_response_data(child_pipelinerun):
    dict_val = {
        'parent_pipelineName': child_pipelinerun.pipeline_name,
        'parent_pipeline_run_id' : child_pipelinerun.pipeline_run_id,
        'activity_name' : child_pipelinerun.activity_name,
        'activity_type' : child_pipelinerun.activity_type,
        'status' : child_pipelinerun.status,
        'pipelineName' :  None if child_pipelinerun.output is None or child_pipelinerun.output.get("pipelineName") is None else child_pipelinerun.output['pipelineName'],
        'pipelineRunId': None if child_pipelinerun.output is None or child_pipelinerun.output.get("pipelineRunId") is None else child_pipelinerun.output['pipelineRunId']
    }
    return dict_val

def traverse_responses_recursively(parent_run_id,data_factory,time_parameters,pipelines_to_cancel = None):
    if pipelines_to_cancel is None:
        pipelines_to_cancel = []

    child_activitys = [get_childpipelines_response_data(val) for val in get_pipeline_responses(parent_run_id,time_parameters,data_factory).value if (val.activity_type=='ExecutePipeline')]
    for child_activity in child_activitys:
        
        if child_activity['pipelineRunId']:
            if child_activity['activity_type'] =='ExecutePipeline':
                cancelPipeline(child_activity['pipelineRunId'],data_factory)
                pipelines_to_cancel.append(child_activity)
                traverse_responses_recursively(child_activity['pipelineRunId'],data_factory,time_parameters,pipelines_to_cancel)
                
    return pipelines_to_cancel

def cancelPipeline(run_id,data_factory):
    client = DataFactoryManagementClient(
    credential=DefaultAzureCredential(),        
    subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"))
    try: 
        client.pipeline_runs.cancel(
                            resource_group_name="rg-ms-etl-prod",
                            factory_name=data_factory,
                            run_id=run_id,
                        )
    except HttpResponseError as ex:
        print (f"run id : {run_id} run is Completed or Cancelled Already Exception:{ex}")

