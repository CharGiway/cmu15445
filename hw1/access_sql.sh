#!/bin/bash
echo ".read /home/caizhw3/code/15445course/$1"
sqlite3 imdb-cmudb2022.db ".read /home/caizhw3/code/15445course/$1"