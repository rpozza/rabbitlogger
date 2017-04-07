#!/bin/bash

sudo mkdir /usr/local/bin/rabbitlogger
sudo cp rabbitmq-objstore.py /usr/local/bin/rabbitlogger/rabbitmq-objstore.py
sudo cp myconfig.json /usr/local/bin/rabbitlogger/myconfig.json
sudo cp rabbitmq-objstore.service /lib/systemd/system/rabbitmq-objstore.service
sudo chmod 644 /lib/systemd/system/rabbitmq-objstore.service
sudo systemctl daemon-reload 
sudo systemctl enable rabbitmq-objstore.service

#reboot and systemctl status rabbitmq-objstore.service

