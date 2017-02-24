#!/bin/sh

# Notes : This needs jq installed use `brew install jq`

# A random name for the collection
collectionName=$(uuidgen)

# Send all messages to flourence which are then redirect to zebedee
host="http://localhost:8081"

# Json message used to create a collection
read -r -d '' collectionJson <<- EOM
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
while [[ $# -gt 1 ]]
do
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
    -z|--zebedee_root)
    ZEBEDEE_ROOT="$2"
    shift # past argument
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done
echo DIRECTORY  = "${DIRECTORY}"
echo Token     = "${TOKEN}"

# traveres collections
filesToSend=$(find ${DIRECTORY} -type f)

# Create collection
echo "Creating Collection : $collectionName"
#547e8b7c34e6954736d731fcbc6a729aafb2f029f665adf906c5df457cca39b4
response=$(curl -sb -X POST --cookie "access_token=${TOKEN}" \
 $host"/zebedee/collection" -d "$collectionJson")

# Find the collectionID in the json message. This is needed to add content
# to the collection. (We also need to remove the quotes from the string)
collectionID=$(echo $response | jq '.id')
collectionID="${collectionID%\"}"
collectionID="${collectionID#\"}"

# Add content to the collection
for file in ${filesToSend[@]}
do
  uri=${file/${DIRECTORY}/}
  if [[ $string == *".json"* ]]; then
    curlUri=$host"/zebedee/content/"$collectionID"?uri="$uri"&overwriteExisting=true&recursive=undefined"
  else
    curlUri=$host"/zebedee/content/"$collectionID"?uri="$uri"&overwriteExisting=true&recursive=undefined&validateJson=false"
  fi
  fileAdded=$(curl -sb -X POST --cookie "access_token=${TOKEN}" $curlUri -d @$file)
  if [ "$fileAdded" == "true" ]
  then
    echo "Added $uri to collection"
  else
    echo "Failed to add $uri to collection"
  fi
done

inprogressDirectory=${ZEBEDEE_ROOT}"zebedee/collections/"${collectionID%-*}"/inprogress/*"
completeDirectory=${ZEBEDEE_ROOT}"zebedee/collections/"${collectionID%-*}"/reviewed"
echo "Moving content to complete"
mv $inprogressDirectory $completeDirectory
echo "Collection ready to be approved"
