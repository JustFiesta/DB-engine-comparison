#!/usr/bin/env bash
# This script installs MongoDB, MariaDB and Docker on Ubuntu 24.04 LTS

# Update system packages
echo ""
echo "======================================================="
echo "Updating system packages..."
sudo apt-get update -y
sudo apt-get upgrade -y

# Install dependencies for adding repositories
echo "Installing dependencies..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# --------------------------------
# Installing MongoDB Repo
# --------------------------------
echo ""
echo "======================================================="
echo "Adding MongoDB repository and key..."

# Import MongoDB public GPG key
curl -fsSL curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

# Add MongoDB repository to apt sources
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

# --------------------------------
# Installing Docker Repo
# --------------------------------
echo ""
echo "======================================================="
echo "Adding Docker repository and key..."

# Add Docker’s official GPG key
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Set up the Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# --------------------------------
# Installing packages
# --------------------------------
# Update apt and install packages
echo ""
echo "======================================================="
echo "Installing Packages..."
sudo apt-get update -y
sudo apt-get install -y zip mariadb-server mongodb-org docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Enable and start Docker service
echo ""
echo "======================================================="
echo "Enabling and starting Docker service..."
sudo systemctl start docker
sudo systemctl enable docker
# Enable and start MongoDB service
echo ""
echo "======================================================="
echo "Enabling and starting MongoDB service..."
sudo systemctl start mongod
sudo systemctl enable mongod

# Verify installations
echo ""
echo "======================================================="
echo "Verifying MongoDB and Docker installation..."
mongod --version
docker --version
mariadb --version

echo ""
echo "======================================================="
echo "MongoDB and Docker have been installed successfully!"
