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
from .app_helper import Helper
from werkzeug.exceptions import HTTPException 

load_dotenv()

views = Blueprint('views', __name__)
CORS(views)



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
# @views.route('/getRecords/<client>', methods=['GET'])
# def getRecords(client):
#     """
#     Fetch records from the database for a specific client.
#     """
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor(dictionary=True)
#         query = "SELECT * FROM etl_datafactory_pipelines WHERE client = %s"
#         cursor.execute(query, (client,))
#         result = cursor.fetchall()
#         cursor.close()
#         return jsonify(result)

#     except Error as e:
#         logging.error(f"Error connecting to MySQL: {str(e)}")
#         return jsonify({"error": str(e)}), 500

#     finally:
#         if 'db' in g:
#             g.db.close()
#             g.pop('db', None)

# @views.route('/getClients', methods=['GET'])
# def getClients():
#     """
#     Fetch records from the database for a specific client.
#     """
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor(dictionary=True)
#         query = "SELECT distinct(client) FROM etl_datafactory_pipelines"
#         cursor.execute(query)
#         result = cursor.fetchall()
#         cursor.close()
#         return jsonify(result)

#     except Error as e:
#         logging.error(f"Error connecting to MySQL: {str(e)}")
#         return jsonify({"error": str(e)}), 500

#     finally:
#         if 'db' in g:
#             g.db.close()
#             g.pop('db', None)



# #Auth server
# @views.route('/login/', methods=['GET', 'POST'])
# def login():
        
#         # if g.user is not None and g.user.is_authenticated:
#         #     return redirect(self.appbuilder.get_url_for_index)
        
#         appBaseURL = os.getenv("APP_BASE_URL") 
#         authNextUrl = os.getenv("AUTH_NEXT_URL")
#         accountServerLoginUrl = os.getenv("AUTH_BASE_URL")  + "auth/login/"
#         # Invoke Auth server URL with authNextUrl set to this controller
#         authNextUrl = appBaseURL + "/login-post"
      
#         authAppName = os.getenv("AUTH_APP_NAME")
#         appNextUrl = os.getenv("APP_NEXT_URL")
#         domainName = os.getenv("DOMAIN_NAME")

#         # Redirect with parameters
#         if domainName:
#             print(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&domainName={domainName}&appNextUrl={appNextUrl}')
#             return redirect(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&domainName={domainName}&appNextUrl={appNextUrl}')
#         else:
#             return redirect(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&appNextUrl={appNextUrl}')


    
# @views.route('/login-post/', methods=['GET', 'POST'])
# def login_post():
#         try:
            
#             print("Entered login post")
#             auth_status = bool(request.args.get('authStatus'))
#             auth_temp_token = request.args.get('authTempToken')
#             auth_message = request.args.get('authMessage')

#             print("tempToken", auth_temp_token)
            
#             response , status= Helper().get_auth_token(auth_status, auth_message, auth_temp_token)
            
#             response = redirect(os.getenv("APP_NEXT_URL"), code=301)  # Permanent redirect
#             return response
        
#         except ApiException as e:
#             # Handle ApiException, you can customize this part
#             return str(e), 400  # Returning a BadRequest status code for ApiException
        
    
# @views.route('/login-post-auth/', methods=['GET', 'POST'])
# def login_post_auth():
#         try:
#             print("Enter in that function ......")
#             auth_token = request.args.get('authToken')
#             print("AuthToken", auth_token)
#             user= Helper().get_user_from_token(auth_token)
#             session["authToken"] = auth_token
#             print("user" , user)
#             user_info = Helper().extract_user_info(user)
            
#             superset_user = self.appbuilder.sm.auth_user_oauth(user_info)
#             print("superset_user", superset_user)
#             login_user(superset_user)
#             g.user.authtoken = auth_token
#             session["domainName"] = user._domainName
#             print(domainName)
#             response = redirect(os.getenv("APP_NEXT_URL"), code=301)  # Permanent redirect
#             return response
        
#         except ApiException as e:
#             # Handle ApiException, you can customize this part
#             return str(e), 400  # Returning a BadRequest status code for ApiException
        

# @views.route('/logout/' , methods=['GET'])
# def logout():
#         try:
#             auth_token = session["authToken"]
#             session.clear()
#             # self.delete_token(auth_token)
#             logout_user()
#             response = redirect(os.getenv("APP_BASE_URL"), code=301)
#             return response
#         except ApiException as e:
#             return str(e) , 400
        
   

# @views.teardown_appcontext
# def close_db_connection(exception=None):
#     db = g.pop('db', None)
#     if db is not None:
#         db.close()

@views.route('/views-test',methods=['GET','POST'])
def views_test():
    print('views-test')
    return 'views-test'


views.route('test',methods=['GET','POST'])
def test():
    print("hello test")
    return 'hello'