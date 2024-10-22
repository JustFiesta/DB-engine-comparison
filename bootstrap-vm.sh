#!/usr/bin/env bash
# This script installs MongoDB and Docker on Ubuntu

# Update system packages
echo "Updating system packages..."
sudo apt-get update -y
sudo apt-get upgrade -y

# Install dependencies for adding repositories
echo "Installing dependencies..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# --------------------------------
# Installing MongoDB Repo
# --------------------------------
echo "Adding MongoDB repository and key..."

# Import MongoDB public GPG key
curl -fsSL https://pgp.mongodb.com/server-6.0.asc | sudo tee /etc/apt/trusted.gpg.d/mongodb-server-6.0.asc

# Add MongoDB repository to apt sources
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list

# --------------------------------
# Installing Docker Repo
# --------------------------------
echo "Adding Docker repository and key..."

# Add Docker’s official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# --------------------------------
# Installing packages
# --------------------------------
# Update apt and install Docker
echo "Installing Docker..."
sudo apt-get update -y
sudo apt-get install -y mariadb-server mongodb-org docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Enable and start Docker service
echo "Enabling and starting Docker service..."
sudo systemctl start docker
sudo systemctl enable docker
# Enable and start MongoDB service
echo "Enabling and starting MongoDB service..."
sudo systemctl start mongod
sudo systemctl enable mongod

# Verify installations
echo "Verifying MongoDB and Docker installation..."
mongod --version
docker --version

echo "MongoDB and Docker have been installed successfully!"
