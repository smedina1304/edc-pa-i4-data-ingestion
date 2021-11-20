#!/bin/bash

echo "-> RUN IN DOCKER..."
echo "PRG_NAME: " $PRG_NAME

cd /app

echo "-> Secrets (copy)..."
mkdir /app/secrets
cp /var/secrets/*.* /app/secrets/*.* 
ls -ls /app/secrets
chmod -R 755 /app/secrets


echo "Executando a chamada..."
pwd
python3 $PRG_NAME