#/usr/bin/bash

VOCAB_FILE='vocabulary_lists.zip'
URL_ROOT='http://www.lawsofthought.org/shared'

echo "wget-ing $VOCAB_FILE from $URL_ROOT"
wget "$URL_ROOT/$VOCAB_FILE"

if [ -f $VOCAB_FILE ]; then
	echo "Un-zipping $VOCAB_FILE"
	unzip $VOCAB_FILE
else
	echo "$VOCAB_FILE does not exist"
fi
