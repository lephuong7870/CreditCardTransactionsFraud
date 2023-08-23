from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import InputRequired, EqualTo

class signUpForm(FlaskForm):
    customer_id = StringField('Customer ID', validators=[InputRequired()])
    password = PasswordField('Password' , validators=[InputRequired()])
    confirm_password = PasswordField('Confirm Password' , validators=[InputRequired(), EqualTo('password')])

    submit = SubmitField('Sign Up')

class loginForm(FlaskForm):
    customer_id = StringField('Customer ID', validators=[InputRequired()])
    password = PasswordField('Password' , validators=[InputRequired()])
    submit = SubmitField('Login')

class loginForm_manager(FlaskForm):
    name = StringField('Name Manager', validators=[InputRequired()])
    password = PasswordField('Password' , validators=[InputRequired()])
    submit = SubmitField('Login')