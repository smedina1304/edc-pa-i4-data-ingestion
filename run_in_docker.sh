#!/bin/bash

echo "-> RUN IN DOCKER..."
echo "PRG_NAME: " $PRG_NAME

cd /app

echo "-> Secrets (copy)..."

ls -ls /var/secrets/credentials
ls -ls /var/secrets/gcp
ls -ls /var/secrets/settings

mkdir /app/secrets

cp /var/secrets/credentials/credentials.json /app/secrets/credentials.json
cp /var/secrets/settings/settings.yaml /app/secrets/settings.yaml

ls -ls /app/secrets
chmod -R 755 /app/secrets

echo "Executando a chamada..."
pwd
python3 $PRG_NAME