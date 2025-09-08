import base64
import datetime
import http.server
import json
import google.auth
from google.auth.transport.urllib3 import Request
import urllib3
import time

def encode(source):
  """Safe base64 encoding."""
  return base64.urlsafe_b64encode(source.encode('utf-8')).decode('utf-8').rstrip('=')

class TokenProvider(object):
  """
  Provides OAuth tokens from Google Cloud Application Default credentials.
  """
  HEADER = json.dumps({'typ':'JWT', 'alg':'GOOG_OAUTH2_TOKEN'})

  def __init__(self, **config):
    self.credentials, _project = google.auth.default()
    self.http_client = urllib3.PoolManager()

  def get_credentials(self):
    if not self.credentials.valid:
      self.credentials.refresh(Request(self.http_client))
    return self.credentials

  def get_jwt(self, creds):
    token_data = dict(
      exp=creds.expiry.replace(tzinfo=datetime.timezone.utc).timestamp(),
      iat=datetime.datetime.now(datetime.timezone.utc).timestamp(),
      iss='Google',
      sub=creds.service_account_email
    )
    return json.dumps(token_data)

  def get_token(self, args):
    creds = self.get_credentials()
    token = '.'.join([
      encode(self.HEADER),
      encode(self.get_jwt(creds)),
      encode(creds.token)
    ])

    # compute expiry time
    expiry_utc = creds.expiry.replace(tzinfo=datetime.timezone.utc)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    expiry_seconds = (expiry_utc - now_utc).total_seconds()

    return token, time.time() + expiry_seconds