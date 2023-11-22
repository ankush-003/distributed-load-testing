#!/bin/bash
start_dir=$(pwd)
sudo echo "Starting Kafka installation"
cd

sudo apt-get update -y
sudo apt-get upgrade -y

# Delete previous zookeeper files and installations
sudo rm -rf kafka*
if [ -d "/usr/local/kafka" ]; then
    echo "Deleting previous Kafka files and installations"
    sudo rm -rf /usr/local/kafka
fi

# Download Kafka
echo "Downloading Kafka binaries"
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz

# Extract Kafka
echo "Decompressing tar archive for Kafka"
tar -xf kafka_2.12-3.6.0.tgz
sudo mv kafka_2.12-3.6.0 /usr/local/kafka

# Setting up zookeeper.service
echo "Setting up zookeeper.service"
if [ -f "/etc/systemd/system/zookeeper.service" ]; then
    sudo rm /etc/systemd/system/zookeeper.service
fi
sudo touch /etc/systemd/system/zookeeper.service
sudo chmod 777 /etc/systemd/system/zookeeper.service

sudo echo "[Unit]" >> /etc/systemd/system/zookeeper.service
sudo echo "Description=Apache Zookeeper server" >> /etc/systemd/system/zookeeper.service
sudo echo "Documentation=http://zookeeper.apache.org" >> /etc/systemd/system/zookeeper.service
sudo echo "Requires=network.target remote-fs.target" >> /etc/systemd/system/zookeeper.service
sudo echo "After=network.target remote-fs.target" >> /etc/systemd/system/zookeeper.service
sudo echo "" >> /etc/systemd/system/zookeeper.service
sudo echo "[Service]" >> /etc/systemd/system/zookeeper.service
sudo echo "Type=simple" >> /etc/systemd/system/zookeeper.service
sudo echo "ExecStart=/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties" >> /etc/systemd/system/zookeeper.service
sudo echo "ExecStop=/usr/local/kafka/bin/zookeeper-server-stop.sh" >> /etc/systemd/system/zookeeper.service
sudo echo "Restart=on-abnormal" >> /etc/systemd/system/zookeeper.service
sudo echo "" >> /etc/systemd/system/zookeeper.service
sudo echo "[Install]" >> /etc/systemd/system/zookeeper.service
sudo echo "WantedBy=multi-user.target" >> /etc/systemd/system/zookeeper.service
sudo echo "" >> /etc/systemd/system/zookeeper.service

# Setting up kafka.service
echo "Setting up kafka.service"
if [ -f "/etc/systemd/system/kafka.service" ]; then
    sudo rm /etc/systemd/system/kafka.service
fi
sudo touch /etc/systemd/system/kafka.service
sudo chmod 777 /etc/systemd/system/kafka.service

sudo echo "[Unit]" >> /etc/systemd/system/kafka.service
sudo echo "Description=Apache Kafka Server" >> /etc/systemd/system/kafka.service
sudo echo "Documentation=http://kafka.apache.org/documentation.html" >> /etc/systemd/system/kafka.service
sudo echo "Requires=zookeeper.service" >> /etc/systemd/system/kafka.service
sudo echo "" >> /etc/systemd/system/kafka.service
sudo echo "[Service]" >> /etc/systemd/system/kafka.service
sudo echo "Type=simple" >> /etc/systemd/system/kafka.service
sudo echo "Environment="JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"" >> /etc/systemd/system/kafka.service
sudo echo "ExecStart=/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties" >> /etc/systemd/system/kafka.service
sudo echo "ExecStop=/usr/local/kafka/bin/kafka-server-stop.sh" >> /etc/systemd/system/kafka.service
sudo echo "" >> /etc/systemd/system/kafka.service
sudo echo "[Install]" >> /etc/systemd/system/kafka.service
sudo echo "WantedBy=multi-user.target" >> /etc/systemd/system/kafka.service
sudo echo "" >> /etc/systemd/system/kafka.service
echo "Successfully created services"

sudo chown -R $USER:$USER /usr/local/kafka
sudo systemctl daemon-reload

# Start zookeeper and kafka
sudo systemctl start zookeeper
sudo systemctl start kafka
sleep 5

echo "---------------"
sudo systemctl is-active --quiet kafka
if [ $? -eq 0 ]; then
    echo "Kafka has been successfully installed and started"
else
    echo "Error in starting Kafka. Check logs at /usr/local/kafka/logs"
fi

echo "---------------"
echo ""
echo "If you wish to stop Kafka, run the following commands"
echo "sudo systemctl stop kafka"

echo "To check the status of Kafka, run the following command"
echo "sudo systemctl status kafka"

cd $start_dir