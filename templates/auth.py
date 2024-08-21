import requests
import os
import logging
import traceback
from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask_cors import CORS
import mysql.connector  # or import pymysql if you're using PyMySQL
from mysql.connector import Error  # or use pymysql
from flask import g  # global context for database connection
from flask import current_app,redirect
# from app_helper import Helper
from werkzeug.exceptions import HTTPException 
from flask_login import login_user, logout_user,LoginManager,UserMixin,login_required,current_user
from flask import session,url_for
from flask_appbuilder import BaseView,AppBuilder
from flask.views import MethodView
from functools import wraps



load_dotenv()

auth = Blueprint('auth', __name__)
CORS(auth,supports_credentials=True)
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    # Other headers can be added here if needed
    return response


#  and request.endpoint not in ('auth.login', 'auth.login_post', 'auth.login_post_auth', 'auth.logout'):
# Global authentication filter
@auth.before_app_request
def global_authentication_filter():
    """This function runs before every request to check if the user is authenticated."""
    print("toekn in global auth is ",request.headers.get('Authorization').split(" ")[0])
    if not is_user_logged_in(request.headers.get('Authorization').split(" ")[0])  and request.endpoint not in ('auth.login', 'auth.login_post', 'auth.logout','auth.currentUser'):
        print("user is authenticated in global auth")
        # Redirect to login if user is not logged in
        # print("redirecting to auth login ",url_for('auth.login'))
        

def is_user_logged_in(authToken):
    """Check if the user is logged in by verifying session or token."""
    user= Helper().get_user_from_token(authToken)
    if user is None:
        return False
    user_info = Helper().extract_user_info(user)
    print("user in global auth is ",user_info)
    if user_info is not None:
        return True
    return False

# @auth.before_request
# def make_session_permanent():
#     session.permanent = True
#     auth.permanent_session_lifetime = timedelta(minutes=30) 



@auth.route('/login/', methods=['GET', 'POST'])
def login():
        
        
        appBaseURL = current_app.config.get("APP_BASE_URL", "") 
        authNextUrl = current_app.config.get("AUTH_NEXT_URL", "")
        accountServerLoginUrl = current_app.config.get("AUTH_BASE_URL", "")  + "auth/login/"
        # Invoke Auth server URL with authNextUrl set to this controller
        authNextUrl = appBaseURL + "/login-post"
      
        authAppName = current_app.config.get("AUTH_APP_NAME", "")
        appNextUrl = current_app.config.get("APP_NEXT_URL", "")
        domainName = current_app.config.get("DOMAIN_NAME", "")

        # Redirect with parameters
        if domainName:
            print(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&domainName={domainName}&appNextUrl={appNextUrl}')
            return redirect(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&domainName={domainName}&appNextUrl={appNextUrl}')
        else:
            return redirect(accountServerLoginUrl + f'?appName={authAppName}&authNextUrl={authNextUrl}&appNextUrl={appNextUrl}')

    
@auth.route('/login-post/', methods=['GET', 'POST'])
def login_post():
        try:
            
            print("Entered login post")
            auth_status = bool(request.args.get('authStatus'))
            auth_temp_token = request.args.get('authTempToken')
            auth_message = request.args.get('authMessage')

            print("tempToken", auth_temp_token)

            response, status = Helper().get_auth_token(auth_status, auth_message, auth_temp_token)
            print("authToken", response.json.get("authToken"))
            session["authToken"] = response.json.get("authToken")
            print("Session: ", session)

            user = Helper().get_user_from_token(response.json.get("authToken"))
            print("user", user)
            user_info = Helper().extract_user_info(user)
            print('user info is ', user_info)
            session["domainName"] = user._domainName
            session['userName']=user_info.get('username')
            session['email']=user_info.get('email')
            print("domain is ", user._domainName)
            from . import User
            user = User(session.get("userName"), session.get("email"))
            
            login_user(user)

            print(f"curentuser login is {current_user}")
            print(f"session is {session}")
            response = redirect(os.getenv("APP_NEXT_URL"), code=301)  # Permanent redirect
            return response
        except Exception as e:
            # Handle ApiException, you can customize this part
            return str(e), 400  # Returning a BadRequest status code for ApiException
        
    
@auth.route('/login-post-auth/', methods=['GET', 'POST'])
def login_post_auth():
        try:
            print("Enter in that function ......")
            auth_token = request.args.get('authToken')
            print("AuthToken", auth_token)
            user= Helper().get_user_from_token(auth_token)
            session["authToken"] = auth_token
            print("user" , user)
            user_info = Helper().extract_user_info(user)
            # response = redirect(current_app.config.get("APP_NEXT_URL"), code=301)  # Permanent redirect
            # return response
            userInfo={
                 "username":user_info.get('userName'),
                 "email":user_info.get('email')
                }
            print('user info in login-post-auth ',userInfo)
            return jsonify(userInfo)
        
        except Exception as e:
            # Handle ApiException, you can customize this part
            return str(e), 400  # Returning a BadRequest status code for ApiException
        

@auth.route('/logout/' , methods=['GET','POST'])
# @login_required
def logout():
        try:
            print(f"current user before logout is ,{current_user}")
            print(f"current user before logout is ,{session}")
           
            logout_user()
            session.clear()
            print("current user after logout is ",current_user)
            print("current user session after logout is",session)
            # response = redirect(os.getenv("APP_BASE_URL"), code=301)
            return jsonify({'message': 'logout successful'}), 200
        except ApiException as e:
            return str(e) , 400
        
@auth.route('/currentUser/',methods=['GET'])
# @login_required
def currentUser():
    # print('Request Headers:', request.headers)
    # # print(' thsi is Cookies: ', request.cookies)
    # print('current user session is ',session)
    userInfo={
        "username":session.get('userName'),
        "email":session.get('email'),
        "auth_token":session.get('authToken')
    }

    print("current user info is ",userInfo)
    return jsonify(userInfo)


class Helper:
    def get_auth_token(self,auth_status,auth_message, auth_temp_token):
        if not auth_status:
            raise Exception(auth_message)

        if auth_temp_token is None:
            raise Exception("Temporary token is not present")

        try:
            # Replace this with the logic to convert the temp token
            qtd = self.convert_temp_token(auth_temp_token)
            print("qtd is ",qtd)
        
        except Exception as e:
            print(e)
            raise Exception

        if not qtd.successful:
            raise Exception

        response = jsonify({"authToken": qtd.token})
        response.set_cookie("authToken", qtd.token, httponly=True, secure=True)
        return response, 200
    
    def get_user_from_token(self,auth_token):

        if auth_token is None:
            raise Exception("Token is not present")

        try:
            # Replace this with the logic to convert the temp token
            user = self.get_user(auth_token)
        
        except Exception as e:
            raise Exception("Error in getting user: " + str(e))
        return user
    

  
    def delete_token(self,auth_token):
        url = f"query/api/token/{auth_token}"
        headers = {}
        headers["authAppToken"] = os.getenv("AUTH_APP_TOKEN")
        response = self.make_request('DELETE' , url , headers)

    def get_user(self, auth_token):
        url = f"query/api/token/{auth_token}"
        headers = {}
        headers["authAppToken"] = os.getenv("AUTH_APP_TOKEN")
        response = self.make_request('GET', url , headers)
        print("response")
        print(response)
        return QueryUserData(**response.json())

    def convert_temp_token(self, temp_token):
        url = f"query/api/temptoken/{temp_token}"
        headers = {}
        headers["authAppToken"] = os.getenv("AUTH_APP_TOKEN")
        print(headers)
        response = self.make_request('GET', url , headers)
        return QueryTokenData(**response.json())

    def make_request(self , method, url, headers=None, params=None, data=None):
        base_url = "https://services.increff.com/account/"  # Replace with your API base URL
        full_url = base_url + url
    
        try:
            response = requests.request(method, full_url, headers=headers, params=params, data=data)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)
        
            return response
        except requests.exceptions.RequestException as e:
            print(e)
            raise Exception(str(e))

    def extract_user_info(self, user_obj):
        """
        Extract user information from an instance of the QueryUserData class
        and return it as a dictionary suitable for auth_user_oauth function.

        :user_obj: An instance of the QueryUserData class
        :return: A dictionary with user information
        """

        print("user object is ",user_obj)
        full_name = user_obj.fullName
        print(full_name)
        first_space_index = full_name.find(" ")

        if first_space_index != -1:
            first_name = full_name[:first_space_index]
            last_name = full_name[first_space_index + 1:]
        else:
            first_name = full_name
            last_name = ""

        user_info = {
            "username": user_obj.username,
            "email": user_obj.email,
            "first_name": first_name,
            "last_name": last_name,
            "roles": user_obj.roles,
            # Add other attributes as needed
        }
        return user_info

#classes from data_types
class QueryUserData:
    def __init__(self, status=False, message="", id=0, username="", email="", fullName="", domainName="",
                 appName="", domainId=0, country="", roles=[], authMode="", phone="", orgName="",
                 resourceRoles={}, *args, **kwargs):
        self._status = status
        self._message = message
        self._id = id
        self._username = username
        self._email = email
        self._fullName = fullName
        self._domainName = domainName
        self._appName = appName
        self._domainId = domainId
        self._country = country
        self._roles = roles
        self._authMode = authMode
        self._phone = phone
        self._orgName = orgName
        self._resourceRoles = resourceRoles

    # Getter and setter for 'status'
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def fullName(self):
        return self._fullName

    @fullName.setter
    def fullName(self, value):
        self._fullName = value

    @property
    def email(self):
        return self._email

    @email.setter
    def email(self, value):
        self._email = value

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value

    # Getter and setter for 'message'
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value

    # Define getters and setters for other attributes similarly

    # For list attributes (roles)
    @property
    def roles(self):
        return self._roles

    @roles.setter
    def roles(self, value):
        self._roles = value

    # For dictionary attributes (resourceRoles)
    @property
    def resourceRoles(self):
        return self._resourceRoles

    @resourceRoles.setter
    def resourceRoles(self, value):
        self._resourceRoles = value


class QueryTokenData:
    def __init__(self, successful=False, token="", message="", *args, **kwargs):
        self.successful = successful
        self.token = token
        self.message = message

    # Getter and setter for 'successful'
    @property
    def successful(self):
        return self._successful

    @successful.setter
    def successful(self, value):
        self._successful = value

    # Getter and setter for 'token'
    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    # Getter and setter for 'message'
    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value


class QueryUserForm:
    def __init__(self,*args, **kwargs):
        self.domainName = None
        self.username = None

    def set_domainName(self, domainName):
        self.domainName = domainName

    def set_username(self, username):
        self.username = username

    def get_domainName(self):
        return self.domainName

    def get_username(self):
        return self.username

@auth.route('test',methods=['GET','POST'])
def test():
    print("hello test")
    return 'hello'