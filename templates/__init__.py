from flask import Flask, session
from .views import views
from .db_util import db_util
from .auth import auth
from flask_login import login_user, logout_user, LoginManager, UserMixin

class User(UserMixin):
    def __init__(self, userName, email):
        self.userName = userName
        self.email = email
    def get_id(self):
        return self.email 
    @staticmethod
    def get(user_id):
        return session  # Assuming session management is handling user data
    def __str__(self):
        return f"User: {self.userName}, Email: {self.email}"

def create_app():
    app = Flask(__name__)
    
    app.config['SECRET_KEY'] = 'fhsjdhsj hfsjfhjs f'
    app.config['SESSION_TYPE'] = 'filesystem'
    
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(db_util, url_prefix='/db')
    app.register_blueprint(auth, url_prefix='/auth')

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)

    return app
