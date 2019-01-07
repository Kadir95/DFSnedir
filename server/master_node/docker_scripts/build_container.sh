#!/bin/bash
sudo docker network create --driver bridge --subnet 172.20.0.0/24 --ip-range 172.20.0.0/24 nedir_network || ture
sudo docker build -t dfsnedir_master -f Dockerfile .