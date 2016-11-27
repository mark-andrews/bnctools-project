#/usr/bin/bash

PKL_FILE='bnc_paragraphs.pkl.bz2'
URL_ROOT='http://www.lawsofthought.org/shared'

echo "wget-ing $PKL_FILE from $URL_ROOT"
wget "$URL_ROOT/$PKL_FILE"

if [ -f $PKL_FILE ]; then
	echo "Un-bz2ing $PKL_FILE"
	bunzip2 $PKL_FILE
else
	echo "$PKL_FILE does not exist"
fi
