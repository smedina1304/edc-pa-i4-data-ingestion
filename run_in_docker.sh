#!/bin/bash

echo "-> RUN IN DOCKER..."
echo "PRG_NAME: " $PRG_NAME

cd /app

echo "Executando a chamada..."
pwd
python3 $PRG_NAME