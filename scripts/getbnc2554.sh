#/usr/bin/bash

BNC_ZIP_ARCHIVE='2554.zip'
BNC_UNZIPPED_DIR='bnc'
URL_ROOT='http://www.lawsofthought.org/shared'

echo "wget-ing $BNC_ZIP_ARCHIVE from $URL_ROOT"
wget "$URL_ROOT/$BNC_ZIP_ARCHIVE"

if [ -f $BNC_ZIP_ARCHIVE ]; then
	echo "Unzipping $BNC_ZIP_ARCHIVE"
	mkdir -p $BNC_UNZIPPED_DIR
	unzip -qu $BNC_ZIP_ARCHIVE -d $BNC_UNZIPPED_DIR
else
	echo "$BNC_ZIP_ARCHIVE does not exist"
fi
