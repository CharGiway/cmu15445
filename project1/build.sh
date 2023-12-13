#!/bin/bash
set -e  # 设置遇到错误即退出

rm -rf build && \
mkdir build  && \
cd build  && \
cmake -DCMAKE_BUILD_TYPE=Debug ..  && \
make -j4