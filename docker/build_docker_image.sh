sudo docker build -t cvapis:9-7 -f Dockerfile-GPU-9-7 .

PORT=8888
MNT=/mnt/ldrive
sudo docker run --runtime=nvidia -it -v $MNT:/root -p $PORT:80 cvapis:9-7 /bin/bash