wsl -d docker-desktop sh -c "sysctl -w vm.max_map_count=262144"
docker-compose up