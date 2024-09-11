from flask import Flask, session
from .views import views
from .db_util import db_util
from .auth import auth
from .error import errors
from flask_login import login_user, logout_user, LoginManager, UserMixin

class User(UserMixin):
    def __init__(self, userName, email):
        self.userName = userName
        self.email = email
    def get_id(self):
        return self.email 
    @staticmethod
    def get(user_id):
        return session 
        
    # @property
    # def is_authenticated(self):
    #     return True # Assuming session management is handling user data
    def __str__(self):
        return f"User: {self.userName}, Email: {self.email}"

def create_app():
    app = Flask(__name__)
    
    app.config['SECRET_KEY'] = 'fhsjdhsj hfsjfhjs f'
    app.config['SESSION_TYPE'] = 'filesystem'
    
    app.register_blueprint(views, url_prefix='/api')
    app.register_blueprint(db_util, url_prefix='/api/db')
    app.register_blueprint(auth, url_prefix='/api/auth')
    app.register_blueprint(errors)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)

    return app
