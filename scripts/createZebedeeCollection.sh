#!/bin/bash

# Notes : This needs jq installed use `brew install jq`

# A random name for the collection
collectionName=$(uuidgen)

# Json message used to create a collection
read -r  -d '' collectionJson <<- EOM
    {
      "name":"$collectionName",
      "type":"manual",
      "publishDate":null,
      "pendingDeletes":[],
      "teams":[],
      "collectionOwner":"PUBLISHING_SUPPORT",
      "releaseUri":null
    }
EOM

# Get args
while [[ $# -gt 1 ]]; do
    key="$1"
    case $key in
        -d|--directory)
            DIRECTORY="$2"
            shift # past argument
            ;;
        -t|--access_token)
            TOKEN="$2"
            shift # past argument
            ;;
        -h|--host)
            HOST="$2"
            shift
            ;;
        -z|--zebedee_root)
            ZEBEDEE_ROOT="$2"
            shift # past argument
            ;;
        *)
            echo Bad arg: $key >&2
            exit 2 # unknown option
            ;;
    esac
    shift # past argument or value
done
echo "DIRECTORY = $DIRECTORY"
echo "Token     = $TOKEN"

# traverse collections
filesToSend=$(find ${DIRECTORY} -type f)

# Create collection
echo "Creating Collection : $collectionName"

response=$(curl -sb -X  POST --cookie "access_token=${TOKEN}" \
 $HOST"/zebedee/collection" -d "$collectionJson" -k)

echo $response
# Find the collectionID in the json message. This is needed to add content
# to the collection. (We also need to remove the quotes from the string)
collectionID=$(echo $response | jq '.id')
collectionID="${collectionID%\"}"
collectionID="${collectionID#\"}"

# Add content to the collection
for file in ${filesToSend[@]}; do
  uri=${file/${DIRECTORY}/}
  curlUri=$HOST"/zebedee/content/"$collectionID"?uri="$uri"&overwriteExisting=true&recursive=undefined"
  if [[ $string != *".json" ]]; then
    curlUri=$curlUri"&validateJson=false"
  fi
  fileAdded=$(curl -sb -X POST --cookie "access_token=${TOKEN}" $curlUri -d @$file -k)
  if [[ "$fileAdded" == "true" ]]; then
    echo "Added $uri to collection"
  else
    echo "Failed to add $uri to collection: $fileAdded"
  fi
done

inprogressDirectory=${ZEBEDEE_ROOT}"zebedee/collections/"${collectionID%-*}"/inprogress/*"
completeDirectory=${ZEBEDEE_ROOT}"zebedee/collections/"${collectionID%-*}"/reviewed"
echo "Moving content to complete $completeDirectory"
mv $inprogressDirectory $completeDirectory
echo "Collection ready to be approved"
