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
VM_SUBNET=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/subnetwork)
echo "VM Subnet: $VM_SUBNET"

echo -e "\n5. MSAK Cluster Info:"
MSAK_SUBNET=$(gcloud managed-kafka clusters describe testpsl --location=us-central1 --format="value(gcpConfig.accessConfig.networkConfigs[0].subnet)" 2>/dev/null)
echo "MSAK Subnet: $MSAK_SUBNET"

echo -e "\n6. Network Match:"
if [ "$VM_SUBNET" = "$MSAK_SUBNET" ]; then
    echo "✅ VM and MSAK on same subnet"
else
    echo "❌ VM and MSAK on different subnets"
    echo "VM: $VM_SUBNET"
    echo "MSAK: $MSAK_SUBNET"
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