#!/bin/bash
elasticUri='http://localhost:9200'
echo "Deleting ONS index"
curl -w "\n" -XDELETE $elasticUri'/ons'
echo "Deleting Departments index"
curl -w "\n" -XDELETE $elasticUri'/departments'
