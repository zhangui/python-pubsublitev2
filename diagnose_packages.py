#!/usr/bin/env python3
"""Diagnose Python package conflicts for MSAK authentication."""

import subprocess
import sys
import pkg_resources
import importlib.metadata

def run_command(cmd):
    """Run a shell command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return "", str(e)

def check_packages():
    """Check installed packages and their versions."""
    print("🔍 Checking installed packages...\n")
    
    # Critical packages for MSAK
    critical_packages = [
        'google-auth',
        'google-auth-httplib2',
        'google-auth-oauthlib',
        'google-cloud-pubsublite',
        'google-cloud-managedkafka',
        'confluent-kafka',
        'urllib3',
        'requests',
        'httplib2',
        'protobuf',
        'grpcio',
        'packaging',
    ]
    
    print("📦 Critical Package Versions:")
    print("-" * 50)
    
    for package in critical_packages:
        try:
            version = importlib.metadata.version(package)
            print(f"✅ {package:30} {version}")
        except importlib.metadata.PackageNotFoundError:
            print(f"❌ {package:30} NOT INSTALLED")
    
    print("\n" + "=" * 50 + "\n")

def check_conflicts():
    """Check for known package conflicts."""
    print("🔍 Checking for known conflicts...\n")
    
    # Check pip check for conflicts
    print("Running pip check for conflicts:")
    stdout, stderr = run_command("pip3 check")
    if stdout:
        print(stdout)
    if stderr:
        print(f"Errors: {stderr}")
    
    if not stdout and not stderr:
        print("✅ No conflicts detected by pip check")
    
    print("\n" + "=" * 50 + "\n")

def check_dependencies():
    """Check package dependencies."""
    print("🔍 Checking package dependencies...\n")
    
    packages_to_check = ['google-auth', 'confluent-kafka', 'urllib3']
    
    for package in packages_to_check:
        print(f"📦 Dependencies for {package}:")
        stdout, stderr = run_command(f"pip3 show {package}")
        if stdout:
            for line in stdout.split('\n'):
                if 'Requires:' in line or 'Required-by:' in line:
                    print(f"   {line}")
        print()
    
    print("=" * 50 + "\n")

def check_duplicate_installations():
    """Check for duplicate package installations."""
    print("🔍 Checking for duplicate installations...\n")
    
    # Get all distributions
    all_packages = {}
    for dist in pkg_resources.working_set:
        name = dist.project_name.lower()
        if name not in all_packages:
            all_packages[name] = []
        all_packages[name].append(f"{dist.version} ({dist.location})")
    
    # Find duplicates
    duplicates = {k: v for k, v in all_packages.items() if len(v) > 1}
    
    if duplicates:
        print("⚠️  Found duplicate packages:")
        for name, versions in duplicates.items():
            print(f"   {name}:")
            for v in versions:
                print(f"      - {v}")
    else:
        print("✅ No duplicate packages found")
    
    print("\n" + "=" * 50 + "\n")

def check_import_conflicts():
    """Try importing key modules to find conflicts."""
    print("🔍 Testing module imports...\n")
    
    test_imports = [
        ('google.auth', 'Google Auth'),
        ('google.auth.transport.urllib3', 'Google Auth urllib3 transport'),
        ('google.auth.transport.requests', 'Google Auth requests transport'),
        ('confluent_kafka', 'Confluent Kafka'),
        ('urllib3', 'urllib3'),
        ('google.cloud.pubsublite', 'Pub/Sub Lite'),
        ('google.cloud.managedkafka_v1', 'Managed Kafka'),
    ]
    
    for module_name, description in test_imports:
        try:
            module = __import__(module_name)
            version = getattr(module, '__version__', 'unknown')
            print(f"✅ {description:35} imported successfully (version: {version})")
        except ImportError as e:
            print(f"❌ {description:35} IMPORT FAILED: {e}")
        except Exception as e:
            print(f"⚠️  {description:35} ERROR: {e}")
    
    print("\n" + "=" * 50 + "\n")

def suggest_fixes():
    """Suggest fixes for common issues."""
    print("💡 Common fixes for package conflicts:\n")
    
    print("1. Clean reinstall of Google Auth packages:")
    print("   pip3 uninstall -y google-auth google-auth-httplib2 google-auth-oauthlib")
    print("   pip3 install google-auth google-auth[urllib3]")
    print()
    
    print("2. Fix urllib3 conflicts:")
    print("   pip3 uninstall -y urllib3")
    print("   pip3 install urllib3==1.26.18")
    print()
    
    print("3. Reinstall Confluent Kafka:")
    print("   pip3 uninstall -y confluent-kafka")
    print("   pip3 install confluent-kafka")
    print()
    
    print("4. Complete clean reinstall (nuclear option):")
    print("   pip3 freeze | xargs pip3 uninstall -y")
    print("   pip3 install google-cloud-pubsublite[msak]")
    print()
    
    print("5. Use virtual environment (recommended):")
    print("   python3 -m venv msak_env")
    print("   source msak_env/bin/activate")
    print("   pip3 install google-cloud-pubsublite[msak]")
    print()

def main():
    print("=" * 60)
    print("🔧 MSAK Package Conflict Diagnosis Tool")
    print("=" * 60)
    print()
    
    # Run all checks
    check_packages()
    check_conflicts()
    check_dependencies()
    check_duplicate_installations()
    check_import_conflicts()
    suggest_fixes()
    
    print("=" * 60)
    print("✅ Diagnosis complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()