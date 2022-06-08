# reset docker container by recreating image
- docker-compose down
- docker system prune -f
- docker rmi -f $(docker images -a -q)
- docker-compose up -d --build

# other useful docker commands
- docker images
- docker ps
- docker logs <container_id>
- docker kill <container_id>
- docker exec -it <container_id> /bin/bash
- docker build . -t <image_name>
- docker run -it -d -p 8080:8080 <image_name>

