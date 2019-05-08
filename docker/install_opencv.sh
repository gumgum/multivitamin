OPENCV_DIR=3.4.3
OPENCV_SRCS=$OPENCV_DIR.tar.gz
OPENCV_SRCS_URL=https://github.com/opencv/opencv/archive/$OPENCV_SRCS

echo "Installing OpenCV $OPENCV_DIR"
cd /opt/
wget $OPENCV_SRCS_URL

tar -zxvf $OPENCV_SRCS
rm -rf $OPENCV_SRCS
cd opencv-$OPENCV_DIR/
rm -rf build/
mkdir build/
cd build/

cmake -D CMAKE_BUILD_TYPE=RELEASE -D CMAKE_INSTALL_PREFIX=/usr/local -D WITH_TBB=ON -D PYTHON3_EXECUTABLE=/usr/bin/python3 -D PYTHON_INCLUDE_DIR=/usr/include/python3.5 \
        -D PYTHON_INCLUDE_DIR2=/usr/include/x86_64-linux-gnu/python3.5m -D PYTHON_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython3.5m.so -D PYTHON3_NUMPY_INCLUDE_DIRS=/usr/lib/python3/dist-packages/numpy/core/include/ \
        -D PYTHON_DEFAULT_EXECUTABLE=/usr/bin/python3 ..

make -j$(($(nproc) + 1))
make install

echo "/usr/local/lib" > /etc/ld.so.conf.d/opencv.conf
ldconfig
