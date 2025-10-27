#!/usr/bin/env python

"""
Debug what type of credentials are available and what attributes they have.
"""

import google.auth

print("\n=== Checking Application Default Credentials ===\n")

try:
    credentials, project = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    print(f"✓ Credentials found")
    print(f"  Project: {project}")
    print(f"  Credential type: {type(credentials).__module__}.{type(credentials).__name__}")
    print(f"  Credential class: {credentials.__class__.__name__}\n")

    print("Credential attributes:")
    attrs = dir(credentials)
    important_attrs = [
        'token', 'expiry', 'valid', 'expired',
        'service_account_email', 'signer_email',
        'refresh_token', 'client_id', 'client_secret'
    ]

    for attr in important_attrs:
        if hasattr(credentials, attr):
            value = getattr(credentials, attr)
            if attr in ['token', 'refresh_token', 'client_secret'] and value:
                print(f"  ✓ {attr}: <redacted>")
            else:
                print(f"  ✓ {attr}: {value}")
        else:
            print(f"  ✗ {attr}: NOT PRESENT")

    print("\n" + "="*60)
    print("Analysis:")
    print("="*60)

    if hasattr(credentials, 'service_account_email'):
        print("✓ This is a SERVICE ACCOUNT credential")
        print(f"  Service account: {credentials.service_account_email}")
    elif hasattr(credentials, 'client_id'):
        print("✓ This is a USER ACCOUNT credential")
        print("  (from 'gcloud auth application-default login')")
    elif 'Compute' in str(type(credentials)):
        print("✓ This is a COMPUTE ENGINE credential")
        print("  (from VM metadata server)")
    else:
        print("? Unknown credential type")

    print("\nFor TokenProvider to work, you need:")
    print("  - service_account_email attribute (for JWT 'sub' field)")

    if not hasattr(credentials, 'service_account_email'):
        print("\n⚠ WARNING: Missing service_account_email!")
        print("  TokenProvider.get_jwt() will fail on line 34")
        print("\nSolutions:")
        print("  1. Use a service account key:")
        print("     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json")
        print("  2. Or modify TokenProvider to handle user credentials")

except Exception as e:
    print(f"✗ No credentials found: {e}")
    print("\nRun one of:")
    print("  gcloud auth application-default login")
    print("  OR")
    print("  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")

print()
