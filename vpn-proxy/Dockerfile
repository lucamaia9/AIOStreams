# Use the official, always-available Ubuntu 22.04 base image
FROM ubuntu:22.04

# Avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install strongswan, its plugins, squid, iptables, and rsyslog
RUN apt-get update && apt-get install -y \
    strongswan \
    libstrongswan-extra-plugins \
    libcharon-extra-plugins \
    squid \
    curl \
    iptables \
    rsyslog \
    && rm -rf /var/lib/apt/lists/*

# Copy our custom configuration and startup script into the image
COPY squid.conf /etc/squid/squid.conf
COPY rsyslog.conf /etc/rsyslog.conf
COPY start.sh /start.sh

# Make the startup script executable
RUN chmod +x /start.sh

# This command will be run when the container starts
CMD ["/start.sh"]