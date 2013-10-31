# Admin for konfab-consumer
# Very bare bones, not designed for production
# Used to enter Media Outlet data on a limited basis
#
# Basically ripped from examples at < https://github.com/mrjoes/flask-admin >

import os, re
from bson.objectid import ObjectId
from pymongo import MongoClient

from flask import Flask, url_for, redirect, render_template, request

from wtforms import form, fields, validators

from flask.ext import admin, login

from flask.ext.admin.form import Select2Widget
from flask.ext.admin.contrib.pymongo import ModelView, filters
from flask.ext.admin.model.fields import InlineFormField, InlineFieldList

# Create application
app = Flask(__name__)

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Create models
client = MongoClient()
db = client.konfab_admin


def read_env(file_location='.env'):
    """ Pulled from Honcho code with minor updates, reads local default
        environment variables from a .env file located in the project root
        directory.
    """
    try:
        with open(file_location) as f:
            content = f.read()
    except IOError:
        content = ''

    for line in content.splitlines():
        m1 = re.match(r'\A([A-Za-z_0-9]+)=(.*)\Z', line)
        if m1:
            key, val = m1.group(1), m1.group(2)
            m2 = re.match(r"\A'(.*)'\Z", val)
            if m2:
                val = m2.group(1)
            m3 = re.match(r'\A"(.*)"\Z', val)
            if m3:
                val = re.sub(r'\\(.)', r'\1', m3.group(1))
            os.environ.setdefault(key, val)


class User():
    def __init__(self, name, password, id, active=True):
        self.id = id
        self.password = password
        self.name = name
        self.active = active

    def is_authenticated(self):
        return True

    def get_id(self):
        return self.id

    def get_password(self):
        return self.password

    def get_name(self):
        return self.name

    def is_active(self):
        return self.active

    def is_anonymous(self):
        return False

    def get_auth_token(self):
        return make_secure_token(self.name, key='deterministic')



#
class KeywordForm(form.Form):
    term = fields.TextField('Term')
    order= fields.IntegerField('Order', default="1")

class HostsForm(form.Form):
    hostname = fields.TextField('Hostname')

class MediaForm(form.Form):
    name = fields.TextField('Name')
    url = fields.TextField('Url')
    woeid = fields.TextField('woeid',description='http://woe.spum.org/ or http://developer.yahoo.com/geo/geoplanet')

    # Form list
    keywords = InlineFieldList(InlineFormField(KeywordForm))
    hosts = InlineFieldList(InlineFormField(HostsForm))

class MediaView(ModelView):
    column_list = ('name', 'url')
    column_sortable_list = ('name', 'url')

    form = MediaForm

    def is_accessible(self):
        return login.current_user.is_authenticated()

class NeighborhoodsForm(form.Form):
    neighborhood = fields.TextField('neighborhoods')

class CitiesForm(form.Form):
    city = fields.TextField('city')
    woeid = fields.TextField('woeid',description='http://woe.spum.org/ or http://developer.yahoo.com/geo/geoplanet')
    neighborhoods = InlineFieldList(InlineFormField(NeighborhoodsForm))

class CitiesView(ModelView):
    column_list = ('city',)
    column_sortable_list = ('city',)

    form = CitiesForm

    def is_accessible(self):
        return login.current_user.is_authenticated()


class LoginForm(form.Form):
    login = fields.TextField(validators=[validators.required()])
    password = fields.PasswordField(validators=[validators.required()])

    def validate_login(self, field):
        user = self.get_user()

        if user.get_name() != self.login.data:
            raise validators.ValidationError('Invalid user')

        if user.get_password() != self.password.data:
            raise validators.ValidationError('Invalid password')

    def get_user(self):
        return allowed


# init login
def init_login():
    login_manager = login.LoginManager()
    login_manager.init_app(app)

    # Create user loader function
    @login_manager.user_loader
    def load_user(user_id):
        return allowed

# Flask views
@app.route('/')
def index():
    return render_template('index.html', user=login.current_user)

@app.route('/login/', methods=('GET', 'POST'))
def login_view():
    form = LoginForm(request.form)
    #if helpers.validate_form_on_submit(form):
    if request.method == 'POST' and form.validate():
        user = form.get_user()
        login.login_user(user)
        return redirect(url_for('index'))

    return render_template('form.html', form=form)


@app.route('/logout/')
def logout_view():
    login.logout_user()
    return redirect(url_for('index'))


if __name__ == '__main__':
    read_env()

    allowed = User(os.environ['USERNAME'], os.environ['PASSWORD'], 1 )

    init_login()

    # Create admin
    admin = admin.Admin(app, 'Kon*Fab Admin')

    # Add views
    admin.add_view(MediaView(db.media, 'Media Outlets', endpoint='media'))
    admin.add_view(CitiesView(db.cities, 'Cities', endpoint='cities'))

    # Start app
    app.run(host='0.0.0.0', port=8888, debug=True)