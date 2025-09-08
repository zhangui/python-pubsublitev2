#!/bin/bash
# MSAK Connectivity Diagnostic Script
# This script tests network connectivity to Google Managed Service for Apache Kafka (MSAK)

echo "=== MSAK Connectivity Diagnostic ==="

HOSTNAME="bootstrap.testpsl.us-central1.managedkafka.ygnahz-dolphin-dev.cloud.goog"
INTERNAL_IP="10.128.0.26"

echo "1. DNS Resolution Test:"
if nslookup $HOSTNAME > /dev/null 2>&1; then
    IP=$(nslookup $HOSTNAME | awk '/^Address: / { print $2 }' | tail -1)
    echo "✅ DNS resolves to: $IP"
else
    echo "❌ DNS resolution failed"
fi

echo -e "\n2. Hostname Port Test:"
if timeout 5 bash -c "</dev/tcp/$HOSTNAME/9092" 2>/dev/null; then
    echo "✅ Port 9092 reachable via hostname"
else
    echo "❌ Port 9092 NOT reachable via hostname"
fi

echo -e "\n3. Internal IP Port Test:"
if timeout 5 bash -c "</dev/tcp/$INTERNAL_IP/9092" 2>/dev/null; then
    echo "✅ Port 9092 reachable via internal IP"
else
    echo "❌ Port 9092 NOT reachable via internal IP"
fi

echo -e "\n4. VM Network Info:"
# Test if metadata service is available
if curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/ > /dev/null 2>&1; then
    echo "✅ Metadata service is available"
    VM_SUBNET_FULL=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/subnetwork)
    VM_SUBNET=$(basename "$VM_SUBNET_FULL")
    echo "VM Subnet: $VM_SUBNET"
    echo "VM Subnet Full Path: $VM_SUBNET_FULL"
else
    echo "❌ Metadata service not available - this might not be a Google Cloud VM"
    echo "Trying alternative method..."
    
    # Alternative: try using gcloud to get VM info
    VM_NAME=$(hostname)
    echo "VM Name (from hostname): $VM_NAME"
    
    # Try to get network info via gcloud
    if command -v gcloud > /dev/null 2>&1; then
        echo "Trying gcloud method..."
        # This will require knowing the zone, but we can try to detect it
        VM_ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null | cut -d/ -f4)
        if [ -n "$VM_ZONE" ]; then
            VM_SUBNET_FULL=$(gcloud compute instances describe $VM_NAME --zone=$VM_ZONE --format="value(networkInterfaces[0].subnetwork)" 2>/dev/null)
            VM_SUBNET=$(basename "$VM_SUBNET_FULL")
            echo "VM Subnet: $VM_SUBNET"
        else
            echo "❌ Cannot determine VM zone"
            VM_SUBNET="unknown"
        fi
    else
        echo "❌ gcloud not available"
        VM_SUBNET="unknown"
    fi
fi

echo -e "\n5. MSAK Cluster Info:"
MSAK_SUBNET_FULL=$(gcloud managed-kafka clusters describe testpsl --location=us-central1 --format="value(gcpConfig.accessConfig.networkConfigs[0].subnet)" 2>/dev/null)
MSAK_SUBNET=$(basename "$MSAK_SUBNET_FULL")
echo "MSAK Subnet: $MSAK_SUBNET"
echo "MSAK Subnet Full Path: $MSAK_SUBNET_FULL"

echo -e "\n6. Network Match:"
if [ "$VM_SUBNET" = "$MSAK_SUBNET" ]; then
    echo "✅ VM and MSAK on same subnet"
else
    echo "❌ VM and MSAK on different subnets"
    echo "VM Subnet: $VM_SUBNET"
    echo "MSAK Subnet: $MSAK_SUBNET"
fi

echo -e "\n7. Simple ping test:"
ping -c 3 $INTERNAL_IP || echo "Ping failed"

echo -e "\n8. All broker IPs test:"
BROKER_IPS=("10.128.0.26" "10.128.0.27" "10.128.0.28" "10.128.0.29")
for ip in "${BROKER_IPS[@]}"; do
    if timeout 3 bash -c "</dev/tcp/$ip/9092" 2>/dev/null; then
        echo "✅ Broker $ip:9092 reachable"
    else
        echo "❌ Broker $ip:9092 NOT reachable"
    fi
done

echo -e "\n9. DNS Server Check:"
echo "DNS servers in use:"
cat /etc/resolv.conf | grep nameserver

echo -e "\n=== Diagnostic Complete ==="