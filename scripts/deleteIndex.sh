#!/bin/bash

ELASTIC_URL=${ELASTIC_URL:-http://localhost:9200}

echo "Deleting ONS index"
curl -w "\n" -XDELETE $ELASTIC_URL'/ons'
echo "Deleting Departments index"
curl -w "\n" -XDELETE $ELASTIC_URL'/departments'
