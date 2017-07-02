#!/bin/bash
# $1 db port
# $2 backup path
# $3 node name
# $4 HTTP credentials
DB_PORT=$1
BACKUP_PATH=$2
NODE_NAME=$3
UPLOAD_CRED=$4

cd $DB_DATA
pg_basebackup -D "$BACKUP_PATH/$(date +%Y-%m-%dT%H-%M-%S)" -F t -R -z -l "$(date -Iseconds)" -w -p $DB_PORT
ls -1 $BACKUP_PATH | sort -r | (while read BACKUP; do
        echo "Uploading backup $BACKUP"
        BACKUP_DATE=$(echo $BACKUP | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})/T\1:\2:\3%2B00:00/')
        BACKUP_FILE=$BACKUP_PATH/$BACKUP/base.tar.gz
        RESULT=$(curl -u$UPLOAD_CRED --write-out %{http_code} --silent --output /dev/null "https://menke.network/pg_backup/base_backup/$NODE_NAME?date=$BACKUP_DATE" --upload-file "$BACKUP_FILE")
        echo "Result: "$RESULT
        if [ "$RESULT" = "201" ]; then
                echo "Removing local backup"
                rm -rf $BACKUP_PATH/$BACKUP
        fi
done)



