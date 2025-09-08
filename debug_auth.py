#!/usr/bin/env python3
"""Debug OAuth authentication for MSAK."""

import os
import sys
sys.path.insert(0, '/Users/yangzhang/Desktop/psl/python-pubsublite')

def debug_auth():
    print("=== MSAK OAuth Authentication Debug ===\n")
    
    # Check environment
    print("1. Environment Check:")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'Not set')}")
    print(f"   GOOGLE_CLOUD_PROJECT: {os.environ.get('GOOGLE_CLOUD_PROJECT', 'Not set')}")
    print()
    
    # Test Google Auth
    print("2. Google Auth Test:")
    try:
        from google.auth import default
        from google.auth.transport.requests import Request
        
        credentials, project = default()
        print(f"   ✓ Default credentials found")
        print(f"   ✓ Project: {project}")
        print(f"   ✓ Credentials type: {type(credentials).__name__}")
        
        # Check if it's a service account or user account
        if hasattr(credentials, 'service_account_email'):
            print(f"   ✓ Service Account: {credentials.service_account_email}")
        elif hasattr(credentials, '_client_id'):
            print(f"   ✓ User Account: {credentials._client_id[:20]}...")
        
        # Test token refresh
        request = Request()
        credentials.refresh(request)
        print(f"   ✓ Token refreshed successfully")
        print(f"   ✓ Token length: {len(credentials.token)}")
        
        # Check scopes
        if hasattr(credentials, '_scopes'):
            print(f"   ✓ Scopes: {credentials._scopes}")
        
    except Exception as e:
        print(f"   ❌ Google Auth failed: {e}")
        return False
    
    # Test MSAK-specific scopes
    print("\n3. MSAK Scope Test:")
    try:
        # Try to get credentials with specific Kafka scopes
        from google.auth import default
        
        # These are the scopes needed for MSAK
        required_scopes = [
            'https://www.googleapis.com/auth/cloud-platform'
        ]
        
        credentials, project = default()
        
        # If credentials support scoping, apply the required scopes
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes(required_scopes)
            print(f"   ✓ Applied required scopes: {required_scopes}")
        
        request = Request()
        credentials.refresh(request)
        print(f"   ✓ Scoped credentials work")
        
    except Exception as e:
        print(f"   ❌ Scope test failed: {e}")
        return False
    
    # Test direct token request for Kafka
    print("\n4. Kafka Token Test:")
    try:
        from google.auth import default
        from google.auth.transport.requests import Request
        
        credentials, project = default()
        
        # Ensure we have the right scopes
        if hasattr(credentials, 'with_scopes'):
            credentials = credentials.with_scopes([
                'https://www.googleapis.com/auth/cloud-platform'
            ])
        
        request = Request()
        credentials.refresh(request)
        
        token = credentials.token
        print(f"   ✓ Token obtained: {token[:20]}...{token[-10:]}")
        print(f"   ✓ Token type: {type(token)}")
        print(f"   ✓ Token valid: {credentials.valid}")
        
        if hasattr(credentials, 'expiry'):
            print(f"   ✓ Token expiry: {credentials.expiry}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Kafka token test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_auth()
    if success:
        print("\n✅ Authentication debug completed successfully!")
        print("The OAuth setup should work. Check MSAK permissions.")
    else:
        print("\n❌ Authentication debug failed!")
        print("Fix the auth issues above before testing MSAK.")