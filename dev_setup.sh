#!/bin/bash

echo "Start clean_all..."
bash -c "source clean.sh && clean_all"
echo "Ended clean_all"

echo "Starting update..."
apt-get update
echo "Ended update"

echo "Installing python3-venv..."
apt-get install python3-venv
echo "Installed python3-venv"

echo "Installing pandoc..."
apt-get install pandoc
echo "Installed pandoc"

function is_package_installed(){
  dpkg-query -W -f='${Status}' $1 2>/dev/null | grep -c "ok installed"
}

echo "Installing google-chrome..."
if [ $(is_package_installed google-chrome-stable) -eq 0 ];
then
  wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
  sh -c 'echo "deb https://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
  apt-get install google-chrome-stable
fi
echo "Installed google-chrome"

echo "Installing docker..."
if [ $(is_package_installed docker-ce) -eq 0 ];
then
  apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
  add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  apt-get install docker-ce docker-ce-cli containerd.io
fi
echo "Installed docker"
