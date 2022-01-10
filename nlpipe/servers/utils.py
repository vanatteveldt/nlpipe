import jwt
import datetime
import os
import socket

from flask import request

STATUS_CODES = {
    'UNKNOWN': 404,
    'PENDING': 202,
    'STARTED': 202,
    'DONE': 200,
    'ERROR': 500
}
ERROR_MIME = 'application/prs.error+text'

SECRET_KEY = None


class LoginFailed(Exception):
    pass


def do_check_auth():
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        raise LoginFailed("No authentication supplied\n")
    if not auth_header.startswith("Token "):
        raise LoginFailed("Incorrectly formatted authorization header\n")
    token = auth_header[len("Token "):]
    try:
        jwt.decode(token, _secret_key())
    except jwt.DecodeError as e:
        raise LoginFailed("Invalid token") from e


def _secret_key():
    global SECRET_KEY
    if SECRET_KEY is None:
        hostid = os.popen("hostid").read().strip()
        hostname = socket.gethostname()
        SECRET_KEY = "__{hostid}_{hostname}"
    return SECRET_KEY


def get_token():
    payload = {
        'version': 1,
        'iat': datetime.datetime.utcnow(),

    }
    return jwt.encode(payload, _secret_key(), algorithm='HS256')