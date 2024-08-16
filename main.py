from templates import create_app
from flask import make_response

from azure.identity import DefaultAzureCredential

from azure.mgmt.datafactory import DataFactoryManagementClient
from flask_cors import CORS

app=create_app()
CORS(app)

def main():
    @app.route('/')
    def index():
        response = make_response('Hello, World!')
        response.headers['Access-Control-Allow-Origin']='*'
        return response

    app.run(debug=True)




if __name__=='__main__':
    main()
    