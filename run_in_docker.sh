#!/bin/bash

echo "-> RUN IN DOCKER..."
echo "PRG_NAME: " $PRG_NAME

cd /app

echo "-> Secrets (copy)..."
chown -R appuser /var/secrets
ls -ls /var/secrets

cat /var/secrets/credentials.json
cat /var/secrets/key.json
cat /var/secrets/settings.yaml

mkdir /app/secrets

cp -R /var/secrets/credentials.json /app/secrets/credentials.json
cp -R /var/secrets/key.json /app/secrets/key.json
cp -R /var/secrets/settings.yaml /app/secrets/settings.yaml

ls -ls /app/secrets
chmod -R 755 /app/secrets


echo "Executando a chamada..."
pwd
python3 $PRG_NAME