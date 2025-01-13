#!/bin/bash

# export CR_PAT=ghp_abc123
# echo $CR_PAT | docker login ghcr.io -u <user_name> --password-stdin
docker context use default
docker compose -f ./database/docker-compose.yml up -d
