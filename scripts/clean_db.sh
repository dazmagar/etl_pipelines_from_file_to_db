#!/bin/bash

images=$(docker images -q)
# if [ -n "$images" ]; then
#    docker rmi $images
# fi

volumes=$(docker volume ls -q)
if [ -n "$volumes" ]; then
    docker volume rm $volumes
fi

# docker system prune -a -f
