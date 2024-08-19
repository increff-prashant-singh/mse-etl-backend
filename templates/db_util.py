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

db_util = Blueprint('db_util', __name__)
CORS(db_util)



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

@db_util.route('/getRecords/<client>', methods=['GET'])
def getRecords(client):
    """
    Fetch records from the database for a specific client.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM etl_datafactory_pipelines WHERE client = %s AND trigger_flag = %s"
        cursor.execute(query, (client,'1'))
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

@db_util.route('/getClients', methods=['GET'])
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

@db_util.route('/db-test',methods=['GET','POST'])
def views_test():
    print('db-test')
    return 'db-test'

