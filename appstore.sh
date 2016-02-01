#!/usr/bin/env bash

BDRE_APPSTORE_REPO=~/bdreappstore
BDRE_APPSTORE_HOME=~/appstore-apps

rm -f -r $BDRE_APPSTORE_HOME
mkdir -p $BDRE_APPSTORE_HOME

#Pull latest apps from the repo
if [ -d "$BDRE_APPSTORE_REPO" ]; then
echo "refresing repo"
cd $BDRE_APPSTORE_REPO
git pull origin master
if [ $? -ne 0 ]
then exit 1
fi
else
echo "cloning repo for first time"
cd ~
git clone https://github.com/sriharshaboda/bdreappstore.git
if [ $? -ne 0 ]
then exit 1
fi
cd $BDRE_APPSTORE_REPO
fi

for d in */ ; do
    echo "checking $d related apps"
        cd "$d"

   for sd in */ ; do
        echo "archiving $sd app"
        zip -r "${sd%?}" ./"$sd"
        if [ $? -ne 0 ]
        then exit 1
        fi
        echo "archiving done"
        done
    mkdir -p $BDRE_APPSTORE_HOME/"$d"
    rm -f $BDRE_APPSTORE_HOME/"$d"/*.zip
    mv ./*.zip $BDRE_APPSTORE_HOME/"$d"
        cd ../
done
