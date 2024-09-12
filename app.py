from templates import create_app
from flask import make_response

from azure.identity import DefaultAzureCredential

from azure.mgmt.datafactory import DataFactoryManagementClient
from flask_cors import CORS

from flask_login import login_user, logout_user,LoginManager,UserMixin,login_required,current_user
from flask import session

app=create_app()
CORS(app,supports_credentials=True)



@app.route('/api')
def index():
    response = make_response('Hello, World!')
    response.headers['Access-Control-Allow-Origin']='*'
    return response

    





   
    