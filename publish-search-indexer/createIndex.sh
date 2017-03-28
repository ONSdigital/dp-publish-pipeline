#!/bin/bash

elasticUri='http://localhost:9200'

echo "Creating ONS Index"
curl -w "\n" -XPUT $elasticUri'/ons/' -d @ons-index/index-settings.json
echo "Creating ONS default mappings"
curl -w "\n" -XPUT $elasticUri'/ons/_default_/_mapping' -d @ons-index/default-mapping.json
echo "Creating departments Index"
curl -w "\n" -XPUT $elasticUri'/departments/' -d @departments-index/index-settings.json
echo "Creating departments default mappings"
curl -w "\n" -XPUT $elasticUri'/departments/_default_/_mapping' -d @departments-index/default-mapping.json

jsonFiles=$(find departments-index/data -type f | grep ".json")

for file in ${jsonFiles[@]}
do
  id=${file:23:${#file}-28}
  echo "Creating document $id"
  curl -w "\n" -XPUT $elasticUri'/departments/departments/'$id -d @$file
done
