#!/bin/bash
# Setup script for MSAK testing on VM following Google's quickstart guide

set -e

echo "🚀 Setting up MSAK client environment..."

# Configuration
PROJECT_ID="ygnahz-eg-codelab"
REGION="us-central1"
CLUSTER_ID="testpsl"
TOPIC="testtopic"

# Set bootstrap server
export BOOTSTRAP="bootstrap.${CLUSTER_ID}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092"
echo "📡 Bootstrap server: $BOOTSTRAP"

# Install Java if not present
echo "📦 Installing Java..."
sudo apt-get update
sudo apt-get install -y default-jre wget unzip

# Download Kafka if not present
if [ ! -d "kafka_2.13-3.7.2" ]; then
    echo "📥 Downloading Kafka..."
    wget -O kafka_2.13-3.7.2.tgz https://dlcdn.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz
    tar xfz kafka_2.13-3.7.2.tgz
fi

# Set Kafka environment
export KAFKA_HOME=$(pwd)/kafka_2.13-3.7.2
export PATH=$PATH:$KAFKA_HOME/bin

# Download Google MSAK authentication library
echo "📥 Downloading MSAK authentication library..."
wget -O release-and-dependencies.zip https://github.com/googleapis/managedkafka/releases/download/v1.0.5/release-and-dependencies.zip
unzip -o release-and-dependencies.zip

# Copy authentication libraries to Kafka lib directory
echo "📁 Installing authentication libraries..."
cp -r release-and-dependencies/* $KAFKA_HOME/libs/

# Update classpath - include all Kafka libs and dependencies
export CLASSPATH=$CLASSPATH:$KAFKA_HOME/libs/*:$KAFKA_HOME/libs/release-and-dependencies/*:$KAFKA_HOME/libs/release-and-dependencies/dependency/*

# Add Jackson JSON libraries explicitly to classpath (fixes JsonFactory issue)
echo "📦 Adding Jackson JSON dependencies..."
JACKSON_LIBS=$(find $KAFKA_HOME/libs -name "*jackson*.jar" | tr '\n' ':')
export CLASSPATH=$CLASSPATH:$JACKSON_LIBS

echo "🔍 Classpath includes:"
echo "   - Kafka core libs: $KAFKA_HOME/libs/*"
echo "   - MSAK auth libs: $KAFKA_HOME/libs/release-and-dependencies/*"  
echo "   - Dependencies: $KAFKA_HOME/libs/release-and-dependencies/dependency/*"
echo "   - Jackson JSON: $JACKSON_LIBS"

# Create client.properties file
echo "📝 Creating client.properties..."
cat > client.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
EOF

echo "✅ Setup complete!"
echo ""

# Debug classpath if needed
echo "🔍 Verifying required JAR files..."
if find $KAFKA_HOME/libs -name "*jackson*core*.jar" | grep -q .; then
    echo "   ✅ Jackson Core found"
else
    echo "   ❌ Jackson Core missing - downloading..."
    wget -P $KAFKA_HOME/libs/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar
fi

if find $KAFKA_HOME/libs -name "*google*cloud*kafka*.jar" | grep -q .; then
    echo "   ✅ Google Cloud Kafka auth found"
else
    echo "   ❌ Google Cloud Kafka auth missing"
fi

echo ""
echo "📋 Testing connection..."
echo "Listing topics..."
kafka-topics.sh --list \
    --bootstrap-server $BOOTSTRAP \
    --command-config client.properties

echo ""
echo "🎯 To produce messages:"
echo "kafka-console-producer.sh --topic $TOPIC --bootstrap-server \$BOOTSTRAP --producer.config client.properties"
echo ""
echo "👂 To consume messages:"
echo "kafka-console-consumer.sh --topic $TOPIC --from-beginning --bootstrap-server \$BOOTSTRAP --consumer.config client.properties"