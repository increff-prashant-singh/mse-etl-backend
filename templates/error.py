from flask import Blueprint
from flask import redirect
from dotenv import load_dotenv
import os
errors=Blueprint('errors',__name__)

@errors.app_errorhandler(404)
def error_404(error):
    return redirect(f"{os.getenv('UI_BASE_URL')}/error?code=404&message=Page%20not%20found")

# 500 - Internal Server Error
@errors.app_errorhandler(500)
def error_500(error):
    return redirect(f"{os.getenv('UI_BASE_URL')}/error?code=500&message=Internal%20server%20error")

# 403 - Forbidden
@errors.app_errorhandler(403)
def error_403(error):
    return redirect(f"{os.getenv('UI_BASE_URL')}/error?code=403&message=Access%20forbidden")

# 400 - Bad Request
@errors.app_errorhandler(400)
def error_400(error):
    return redirect(f"{os.getenv('UI_BASE_URL')}/error?code=400&message=Bad%20request")

# 401 - Unauthorized
@errors.app_errorhandler(401)
def error_401(error):
    return redirect(f"{os.getenv('UI_BASE_URL')}/error?code=401&message=Unauthorized%20access")