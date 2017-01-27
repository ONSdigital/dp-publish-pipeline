#!/bin/bash

echo "Creating ONS Index"
curl -XPUT 'http://localhost:9200/ons' -d @index-settings.json
echo ""
echo "Creating default mappings"
curl -XPUT 'http://localhost:9200/ons/_default_/_mapping' -d @default-mapping.json
echo ""
