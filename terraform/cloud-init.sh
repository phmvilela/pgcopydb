#!/bin/bash
set -e

# Cloud-init script for pg_cyan migration worker VM
# Ubuntu 22.04 LTS
# Installs: Docker, PostgreSQL client, pg_cyan dependencies

echo "[$(date)] Starting pg_cyan worker VM provisioning..."

# Update system
apt-get update
apt-get upgrade -y

# Install system dependencies
apt-get install -y \
    curl \
    wget \
    git \
    build-essential \
    python3-dev \
    python3-pip \
    libpq-dev \
    docker.io \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release

# Add current user to docker group
usermod -aG docker azureuser

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Install PostgreSQL client for PostgreSQL ${postgresql_version}
echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get update
apt-get install -y postgresql-client-${postgresql_version}

# Install PostgreSQL server (optional, for testing)
apt-get install -y postgresql-${postgresql_version}

# Start PostgreSQL service
systemctl start postgresql
systemctl enable postgresql

# Install Python dependencies for pg_cyan
apt-get install -y python3-pip
pip3 install --upgrade pip

# Create a home directory for pg_cyan
mkdir -p /opt/pg_cyan
cd /opt/pg_cyan

# Clone pg_cyan (placeholder - adjust URL as needed)
# git clone https://your-repo-url/pg_cyan.git . || echo "Repository not yet available"

# Set up Python environment
python3 -m venv /opt/pg_cyan/venv
source /opt/pg_cyan/venv/bin/activate

# Install pg_cyan dependencies
# pip install -r requirements.txt (when repository is available)

echo "[$(date)] PostgreSQL ${postgresql_version} and pg_cyan dependencies installed successfully."
echo "[$(date)] VM is ready for migration tasks."

# Log provisioning success
cat > /var/log/pg_cyan_provisioning.json <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "status": "provisioning_complete",
  "postgresql_version": "${postgresql_version}",
  "docker_available": true,
  "python_environment": "/opt/pg_cyan/venv"
}
EOF

echo "Provisioning log written to /var/log/pg_cyan_provisioning.json"
