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
        print(f"🔐 Initializing TokenProvider...")
        try:
            self.credentials, _project = google.auth.default()
            self.http_client = urllib3.PoolManager()
            print(f"   ✅ Google credentials obtained")
            print(f"   📋 Project: {_project}")
            print(f"   🔑 Credential type: {type(self.credentials).__name__}")
            print(f"   📧 Service account: {getattr(self.credentials, 'service_account_email', 'N/A')}")
            print(f"   ✅ TokenProvider initialized successfully")
        except Exception as e:
            print(f"   ❌ TokenProvider initialization failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    def get_credentials(self):
        print(f"   🔍 Checking credential validity...")
        print(f"   ✅ Current valid status: {self.credentials.valid}")
        
        if not self.credentials.valid:
            print(f"   🔄 Credentials invalid, refreshing...")
            try:
                self.credentials.refresh(Request(self.http_client))
                print(f"   ✅ Credentials refreshed successfully")
                print(f"   📅 New expiry: {self.credentials.expiry}")
                print(f"   🔑 Token length: {len(self.credentials.token) if self.credentials.token else 0}")
            except Exception as e:
                print(f"   ❌ Credential refresh failed: {e}")
                raise
        else:
            print(f"   ✅ Credentials are already valid")
            print(f"   📅 Expires: {self.credentials.expiry}")
            
        return self.credentials

    def get_jwt(self, creds):
        print(f"   🔨 Creating JWT payload...")
        
        try:
            exp_timestamp = creds.expiry.replace(tzinfo=datetime.timezone.utc).timestamp()
            iat_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
            service_email = getattr(creds, 'service_account_email', 'unknown')
            
            token_data = dict(
                exp=exp_timestamp,
                iat=iat_timestamp,
                iss='Google',
                sub=service_email
            )
            
            print(f"   📋 JWT payload:")
            print(f"      iss: {token_data['iss']}")
            print(f"      sub: {token_data['sub']}")
            print(f"      exp: {exp_timestamp} ({datetime.datetime.fromtimestamp(exp_timestamp)})")
            print(f"      iat: {iat_timestamp} ({datetime.datetime.fromtimestamp(iat_timestamp)})")
            
            jwt_payload = json.dumps(token_data)
            print(f"   ✅ JWT payload created (length: {len(jwt_payload)})")
            
            return jwt_payload
            
        except Exception as e:
            print(f"   ❌ JWT creation failed: {e}")
            raise

    def get_token(self, args):
        """
        Get OAuth token for MSAK authentication.
        
        Official Google implementation that creates a JWT token.
        """
        try:
            print(f"🔄 Token requested by confluent-kafka (official Google method)")
            print(f"   📥 Args: {args}")
            
            # Step 1: Get credentials
            creds = self.get_credentials()
            
            # Step 2: Create JWT components
            print(f"   🔧 Building JWT token...")
            
            header_encoded = encode(self.HEADER)
            print(f"   🏷️  Header encoded: {header_encoded}")
            
            jwt_payload = self.get_jwt(creds)
            payload_encoded = encode(jwt_payload)
            print(f"   📦 Payload encoded: {payload_encoded}")
            
            signature_encoded = encode(creds.token)
            print(f"   ✍️  Signature (access token) encoded: {signature_encoded[:50]}...")
            
            # Step 3: Create JWT token exactly like Google's official example
            token = '.'.join([header_encoded, payload_encoded, signature_encoded])
            print(f"   🔗 JWT assembled: {len(token)} characters")

            # Step 4: Compute expiry time exactly like Google's example
            expiry_utc = creds.expiry.replace(tzinfo=datetime.timezone.utc)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            expiry_seconds = (expiry_utc - now_utc).total_seconds()
            expiry_timestamp = time.time() + expiry_seconds
            
            print(f"   ⏰ Token expires in {expiry_seconds:.0f} seconds")
            print(f"   ✅ Final token: {token[:100]}...")
            print(f"   📤 Returning to confluent-kafka")
            
            return token, expiry_timestamp
            
        except Exception as e:
            print(f"   💥 Token provider error: {e}")
            import traceback
            traceback.print_exc()
            return "", 0