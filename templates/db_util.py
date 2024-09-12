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
from flask import g ,session # global context for database connection
from flask import current_app,redirect
from werkzeug.exceptions import HTTPException 
from flask_login import login_required
import json
from datetime import datetime
from .auth import Helper
from flask import abort

load_dotenv()

db_util = Blueprint('db_util', __name__)
CORS(db_util,supports_credentials=True)

@db_util.before_request
def global_authentication_filter():
      if request.method == 'OPTIONS':
        # This is the preflight request; return 200 OK
         return '', 200
#     """This function runs before every request to check if the user is authenticated."""
      authorization_header = request.headers.get('Authorization')
#     print(f"Authorization header for {request.url} is: {authorization_header}")
      if(session.get('authToken') is None):
            session['authToken']=authorization_header
      else: authorization_header=session.get('authToken')
      auth_token = session.get('authToken')
      if not is_user_logged_in(authorization_header):
            abort(401)

def is_user_logged_in(authToken):
    """Check if the user is logged in by verifying session or token."""

    if authToken is None:
        return False

    user = Helper().get_user_from_token(authToken)
    print("user in login check is ",user)
    if user.email is None:
        return False

    user_info = Helper().extract_user_info(user)
    print("User in global auth is:", user_info)

    return user_info is not None

def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    # Other headers can be added here if needed
    return response

# Function to get a database connection
# @login_required
def get_db_connection():
    if 'db' not in g:
        g.db = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DB')
        )
    return g.db

# @login_required
@db_util.route('/getRecords/<client>', methods=['GET','POST'])
def getRecords(client):
    """
    Fetch records from the database for a specific client.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM etl_datafactory_pipelines WHERE client = %s "
        cursor.execute(query, (client,))
        result = cursor.fetchall()
        cursor.close()
        return jsonify(result)

    except Error as e:
        logging.error(f"Error connecting to MySQL: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if 'db' in g:
            g.db.close()
            g.pop('db', None)

# @login_required
@db_util.route('/getClients', methods=['GET','POST'])
def getClients():
    """
    Fetch records from the database for a specific client.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT distinct(client) FROM etl_datafactory_pipelines"
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return jsonify(result)

    except Error as e:
        logging.error(f"Error connecting to MySQL: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if 'db' in g:
            g.db.close()
            g.pop('db', None)

@db_util.route('/AddRunDetails', methods=['GET', 'POST'])
def addRunDetails():
    try:
        # Extract the 'runDetails' argument from the request
        runDetails = request.args.get('runDetails')

        if not runDetails:
            return jsonify({"error": "runDetails parameter is missing"}), 400

        # Parse the runDetails as a dictionary (assuming it is passed as a JSON string)
        runDetails = json.loads(runDetails)
        
        # Extract client name and pipeline name from the runDetails dictionary
        client = runDetails.get('client')
        data_factory = runDetails.get('data_factory')
        pipeline_name = runDetails.get('pipeline_name')
        parameters = str(runDetails.get('parameters'))
        current_timestamp = datetime.now()
        timestamp= current_timestamp.strftime('%Y-%m-%d %H:%M:%S') 
        pipeline_run_id = runDetails.get('pipeline_run_id')
        trigger_status = runDetails.get('trigger_status')
        triggered_by = runDetails.get('triggered_by')
        

        if not client or not pipeline_name:
            return jsonify({"error": "Client or pipeline_name is missing in runDetails"}), 400

        # Establish a connection to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Define the SQL INSERT query
        query = """
            INSERT INTO etl_pipeline_trigger_logs (client,data_factory, pipeline_name,parameters,timestamp,pipeline_run_id,trigger_status, triggered_by)
            VALUES (%s, %s, %s,%s,%s,%s,%s,%s)
        """
        
        # Execute the INSERT query with the provided values
        cursor.execute(query, (client,data_factory, pipeline_name,parameters,timestamp,pipeline_run_id,trigger_status,triggered_by))
        conn.commit()

        # Close the cursor and connection
        cursor.close()

        return jsonify({"message": "Run details added successfully"}), 200

    except mysql.connector.Error as e:
        logging.error(f"Error connecting to MySQL: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if 'db' in g:
            g.db.close()
            g.pop('db', None)

@db_util.route('/db-test',methods=['GET','POST'])
def views_test():
    print('db-test')
    return 'db-test'

