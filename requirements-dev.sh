#!/bin/bash

sudo apt-get update

if [ $(dpkg-query -W -f='${Status}' google-chrome 2>/dev/null | grep -c "ok installed") -eq 0 ];
then
  wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
  sudo sh -c 'echo "deb https://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
  sudo apt-get install google-chrome-stable
fi

sudo apt-get install pandoc
pip install -r requirements-dev.txt
