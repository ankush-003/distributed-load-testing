#!/bin/bash

# shifting to home directory
start_dir=$(pwd)
cd

echo "======================"
echo " Installing GO "
echo "======================"

sudo apt-get update -y
sudo apt-get upgrade -y

echo "======================"
echo " Downloafing GO installation file "
echo "======================"

wget https://go.dev/dl/go1.21.3.linux-amd64.tar.gz

tar -xvf go1.21.3.linux-amd64.tar.gz

if [ -d "/usr/local/go" ]; then
	echo "======================"
	echo "Deleting previous installation"
	echo "======================"
	sudo rm -rf /usr/local/go
fi

sudo mv go /usr/local

echo "export GOROOT=/usr/local/go" >> ~/.bashrc
echo "export GOPATH=\$HOME/go" >> ~/.bashrc
echo "export PATH=\$PATH:\$GOPATH/bin:\$GOROOT/bin" >> ~/.bashrc
source ~/.bashrc

echo "======================"
echo " Installation Complete "
go version
echo "======================"

# By
# ANKUSH H V
