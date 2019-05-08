apt-get update && apt-get -y upgrade
apt-get -y dist-upgrade

echo "Installing nvidia driver 384"
add-apt-repository ppa:graphics-drivers/ppa
apt install -y nvidia-384

echo "Installing docker"
apt-get -y install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get -y update
apt-get -y install docker-ce=18.03.0~ce-0~ubuntu

echo "Installing nvidia-docker runtime"
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | \
  apt-key add -
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | \
  tee /etc/apt/sources.list.d/nvidia-docker.list
apt-get -y update

apt-get install -y nvidia-docker2
pkill -SIGHUP dockerd