"""Google Cloud Token Provider for MSAK authentication - official Google implementation."""

import base64
import datetime
import json
import time
import google.auth
from google.auth.transport.requests import Request
import urllib3

def encode(source):
    """Safe base64 encoding."""
    return base64.urlsafe_b64encode(source.encode('utf-8')).decode('utf-8').rstrip('=')

class TokenProvider(object):
    """
    Provides OAuth tokens from Google Cloud Application Default credentials.
    Official Google implementation from quickstart-python docs.
    """
    HEADER = json.dumps({'typ':'JWT', 'alg':'GOOG_OAUTH2_TOKEN'})

    def __init__(self, **config):
        self.credentials, _project = google.auth.default()
        self.http_client = urllib3.PoolManager()
        print(f"🔐 TokenProvider initialized (official Google implementation)")

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
        """
        Get OAuth token for MSAK authentication.
        
        Official Google implementation that creates a JWT token.
        """
        try:
            print(f"🔄 Token requested by confluent-kafka (official Google method)")
            
            creds = self.get_credentials()
            
            # Create JWT token exactly like Google's official example
            token = '.'.join([
                encode(self.HEADER),
                encode(self.get_jwt(creds)),
                encode(creds.token)
            ])

            # Compute expiry time exactly like Google's example
            expiry_utc = creds.expiry.replace(tzinfo=datetime.timezone.utc)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            expiry_seconds = (expiry_utc - now_utc).total_seconds()

            expiry_timestamp = time.time() + expiry_seconds
            
            print(f"   ✅ JWT token created (official Google format)")
            print(f"   🔤 Token prefix: {token[:80]}...")
            print(f"   ⏰ Token expires in {expiry_seconds:.0f} seconds")
            
            return token, expiry_timestamp
            
        except Exception as e:
            print(f"   💥 Token provider error: {e}")
            import traceback
            traceback.print_exc()
            return "", 0