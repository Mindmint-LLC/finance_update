
#%% Import Libraries

import os
from flask import Blueprint, session, request, redirect, url_for
from google_auth_oauthlib.flow import Flow
import requests


gauth_bp = Blueprint('gauth', __name__)


#%% Google OAuth Functions

CLIENT_SECRETS_FILE = os.getenv('GAUTH_SECRETS_FILE')
SCOPES=["openid", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]


#%% Google OAuth Routes

@gauth_bp.route('/login')
def login():
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for('gauth.authorize', _external=True, _scheme='https')
    )
    authorization_url, state = flow.authorization_url()
    session['state'] = state
    return redirect(authorization_url)


@gauth_bp.route('/authorize')
def authorize():
    try:
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRETS_FILE, 
            scopes=SCOPES, 
            state=session.get('state'),
            redirect_uri=url_for('gauth.authorize', _external=True, _scheme='https')
        )
        authorization_response = request.url.replace('http://', 'https://')
        flow.fetch_token(authorization_response=authorization_response)
        credentials = flow.credentials
        userinfo_response = requests.get(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            headers={'Authorization': f'Bearer {credentials.token}'}
        )
        userinfo_response.raise_for_status()
        if userinfo_response.ok:
            userinfo = userinfo_response.json()
            session["user_email"] = userinfo.get("email")
            session["user_logged_in"] = True
        else:
            raise Exception('Failed to get user email')
        return redirect(url_for('home'))
    except Exception as e:
        session.clear()
        return str(e), 500


#%%