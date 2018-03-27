#!/bin/bash
# Start SSH daemon. Because Apache Spark Standalone connects to worker instance via SSH
# Author: jobusam
# Work: Masterthesis

echo "Start SSH service"
echo "Passwordless ssh connect to localhost is assumed for running Spark!"
sudo systemctl start sshd.service
