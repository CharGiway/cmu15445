#!/bin/bash
cd build && make format && make check-lint && make check-clang-tidy-p1 && make buffer_pool_manager_instance_test && ./test/buffer_pool_manager_instance_test && cd .. && ./submit.sh