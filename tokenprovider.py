"""Google Cloud Token Provider for MSAK authentication - based on official Google example."""

import base64
import datetime
import json
import time
import google.auth
from google.auth.transport.requests import Request

def encode(source):
    """Safe base64 encoding."""
    return base64.urlsafe_b64encode(source.encode('utf-8')).decode('utf-8').rstrip('=')

class TokenProvider(object):
    """Provides OAuth tokens from Google Cloud Application Default credentials."""
    HEADER = json.dumps({'typ':'JWT', 'alg':'GOOG_OAUTH2_TOKEN'})

    def __init__(self, **config):
        """Initialize token provider."""
        self.credentials, self.project = google.auth.default()
        
        # Apply cloud-platform scope
        if hasattr(self.credentials, 'with_scopes'):
            self.credentials = self.credentials.with_scopes([
                'https://www.googleapis.com/auth/cloud-platform'
            ])
        
        self.last_refresh = 0
        print(f"🔐 TokenProvider initialized for project: {self.project}")

    def get_token(self, config_str=None):
        """
        Get OAuth token for MSAK authentication.
        
        This method is called by confluent-kafka when authentication is needed.
        Returns (token, expiry_timestamp) tuple.
        """
        try:
            print(f"🔄 Token requested by confluent-kafka")
            
            # Refresh credentials if needed
            now = time.time()
            if now - self.last_refresh > 3300:  # Refresh every 55 minutes
                print("   📡 Refreshing Google Cloud credentials...")
                request = Request()
                self.credentials.refresh(request)
                self.last_refresh = now
                print("   ✅ Credentials refreshed")
            
            # Get current token
            token = self.credentials.token
            if not token:
                print("   ❌ No access token available")
                return "", 0
            
            # Calculate expiry
            if hasattr(self.credentials, 'expiry') and self.credentials.expiry:
                expiry_timestamp = self.credentials.expiry.timestamp()
                expiry_str = self.credentials.expiry.strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f"   ⏰ Token expires: {expiry_str}")
            else:
                # Default to 1 hour if no expiry info
                expiry_timestamp = time.time() + 3600
                print("   ⏰ No expiry info, defaulting to +1 hour")
            
            # Create the JWT-style token that MSAK expects
            payload = json.dumps({
                'iss': self.credentials.service_account_email if hasattr(self.credentials, 'service_account_email') else 'unknown',
                'sub': self.credentials.service_account_email if hasattr(self.credentials, 'service_account_email') else 'unknown',
                'aud': 'kafka',
                'iat': int(time.time()),
                'exp': int(expiry_timestamp),
                'access_token': token
            })
            
            # Encode as JWT-style token (header.payload.signature)
            jwt_token = f"{encode(self.HEADER)}.{encode(payload)}.{encode('GOOG_OAUTH2_TOKEN')}"
            
            print(f"   ✅ JWT token created (length: {len(jwt_token)})")
            print(f"   🔤 Token prefix: {jwt_token[:100]}...")
            
            return jwt_token, expiry_timestamp
            
        except Exception as e:
            print(f"   💥 Token provider error: {e}")
            import traceback
            traceback.print_exc()
            return "", 0