#!/bin/bash

curl -XPUT 'http://localhost:9200/ons' -d @index-settings.json

# Add a blank line
echo ""
