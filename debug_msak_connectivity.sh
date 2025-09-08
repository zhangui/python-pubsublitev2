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
    
    # Debug: List available network interfaces
    echo "Available network interfaces:"
    curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/
    
    # Try to get network info step by step
    echo "Getting network info..."
    
    # Test each endpoint individually
    echo "Testing network endpoint..."
    VM_NETWORK=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/network 2>/dev/null)
    echo "Network result: '$VM_NETWORK'"
    
    echo "Testing subnetwork endpoint..."  
    VM_SUBNET_FULL=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/subnetwork 2>/dev/null)
    echo "Subnetwork result: '$VM_SUBNET_FULL'"
    
    # Check if subnetwork endpoint exists at all
    SUBNET_HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/subnetwork)
    echo "Subnetwork HTTP code: $SUBNET_HTTP_CODE"
    
    if [ -n "$VM_SUBNET_FULL" ]; then
        VM_SUBNET=$(basename "$VM_SUBNET_FULL")
        echo "VM Network: $(basename "$VM_NETWORK")"
        echo "VM Subnet: $VM_SUBNET"
        echo "VM Subnet Full Path: $VM_SUBNET_FULL"
    else
        echo "⚠️  Subnetwork not available from metadata (likely auto-mode network)"
        echo "VM Network: $(basename "$VM_NETWORK")"
        
        # For auto-mode networks, assume 'default' subnet
        if [[ "$VM_NETWORK" == *"default"* ]]; then
            VM_SUBNET="default"
            echo "VM Subnet: $VM_SUBNET (inferred from auto-mode default network)"
            echo "VM Subnet Range: 10.128.0.0/20 (default auto-mode subnet for this region)"
        else
            # Fallback: try gcloud method
            VM_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null)
            VM_ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null | cut -d/ -f4)
            
            if [ -n "$VM_NAME" ] && [ -n "$VM_ZONE" ]; then
                echo "Trying gcloud method with VM: $VM_NAME in zone: $VM_ZONE"
                VM_SUBNET_FULL=$(gcloud compute instances describe "$VM_NAME" --zone="$VM_ZONE" --format="value(networkInterfaces[0].subnetwork)" 2>/dev/null)
                VM_SUBNET=$(basename "$VM_SUBNET_FULL")
                echo "VM Subnet (via gcloud): $VM_SUBNET"
            else
                VM_SUBNET="default"
                echo "VM Subnet: $VM_SUBNET (fallback assumption)"
            fi
        fi
    fi
else
    echo "❌ Metadata service not available"
    VM_SUBNET="unknown"
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

echo -e "\n8. IP Range Check:"
echo "VM should be in 10.128.0.0/20 range, MSAK brokers at 10.128.0.26-29"
VM_IP=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip 2>/dev/null)
echo "VM Internal IP: $VM_IP"

echo -e "\n9. All broker IPs test:"
BROKER_IPS=("10.128.0.26" "10.128.0.27" "10.128.0.28" "10.128.0.29")
for ip in "${BROKER_IPS[@]}"; do
    echo "Testing $ip:9092..."
    if timeout 5 bash -c "</dev/tcp/$ip/9092" 2>/dev/null; then
        echo "✅ Broker $ip:9092 reachable"
    else
        echo "❌ Broker $ip:9092 NOT reachable"
        # Also test if we can ping it
        if ping -c 1 -W 2 $ip > /dev/null 2>&1; then
            echo "   But ping to $ip works (IP reachable, port 9092 blocked/closed)"
        else
            echo "   Ping to $ip also fails (IP not reachable)"
        fi
    fi
done

echo -e "\n9. DNS Server Check:"
echo "DNS servers in use:"
cat /etc/resolv.conf | grep nameserver

echo -e "\n=== Diagnostic Complete ==="