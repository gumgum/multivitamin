cd /usr/lib/x86_64-linux-gnu/
ln -s libboost_python-py35.so libboost_python3.so

cd /opt/
git clone https://github.com/weiliu89/caffe.git
cd caffe
git checkout ssd
pip3 install -r python/requirements.txt
wget https://s3.amazonaws.com/cv-cuda/Makefile.config
sed -i -e 's/hdf5/hdf5_serial/g' Makefile && \
make -j$(($(nproc) + 1)) && \
make pycaffe && \
make distribute