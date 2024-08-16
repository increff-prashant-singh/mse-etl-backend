from flask import Flask
from .views import views
from .db_util import db_util
from .auth import auth
def create_app():
    app=Flask(__name__)
    app.config['SECRET_KEY']='fhsjdhsj hfsjfhjs f'

    app.register_blueprint(views,url_prefix='/')
    app.register_blueprint(db_util,url_prefix='/db')
    app.register_blueprint(auth,url_prefix='/auth')
    return app

