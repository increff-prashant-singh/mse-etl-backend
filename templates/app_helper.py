from flask import current_app, redirect, g, flash, request, Flask, url_for, session , jsonify

from flask_appbuilder.security.views import UserDBModelView,AuthDBView
from flask_appbuilder.security.views import expose
from flask_appbuilder.security.manager import BaseSecurityManager
from flask_login import login_user, logout_user
import requests



# from data_types import QueryUserData, QueryTokenData, QueryUserForm




class Helper:
    def get_auth_token(self,auth_status, auth_message, auth_temp_token):
        if not auth_status:
            raise ApiException(auth_message)

        if auth_temp_token is None:
            raise ApiException("Temporary token is not present")

        try:
            # Replace this with the logic to convert the temp token
            qtd = self.convert_temp_token(auth_temp_token)
            print(qtd)
        
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
            raise ApiException("Token is not present")

        try:
            # Replace this with the logic to convert the temp token
            user = self.get_user(auth_token)
        
        except Exception as e:
            raise ApiException("Error in getting user: " + str(e))
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
            raise ApiException(str(e))

    def extract_user_info(self, user_obj):
        """
        Extract user information from an instance of the QueryUserData class
        and return it as a dictionary suitable for auth_user_oauth function.

        :user_obj: An instance of the QueryUserData class
        :return: A dictionary with user information
        """

        print(user_obj)
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