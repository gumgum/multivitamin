apt-get update && apt-get upgrade
apt-get dist-upgrade
apt-get install -y build-essential wget zlib1g-dev \
                   cmake git pkg-config curl ca-certificates \
                   libncursesw5-dev libgdbm-dev libc6-dev \
                   zlib1g-dev tk-dev libssl-dev openssl \
                   libffi-dev libreadline-gplv2-dev  \
                   libsqlite3-dev libbz2-dev vim python3-dev python3-pip libgnutls-dev

apt-get install -y --no-install-recommends libgtk2.0-dev libavcodec-dev libavformat-dev libswscale-dev \
        libtbb2 libtbb-dev libjpeg-dev libpng-dev libtiff-dev libjasper-dev libdc1394-22-dev ffmpeg x264 \
        libprotobuf-dev liblmdb-dev libsnappy-dev libhdf5-serial-dev protobuf-compiler \
        libboost-all-dev libopenblas-dev libgflags-dev libgoogle-glog-dev \
        python3-botocore python3-boto3 python3-scipy libmysqlclient-dev

