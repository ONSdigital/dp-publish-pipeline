#!/usr/bin/env bash

# Notes : This needs jq installed use `brew install jq`

log() { echo $(date '+%Y-%m-%d %T') "$@"; }
testing=
max_running=50
max_errors=1

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
        -p|--parallel)
            max_running=$2
            shift # past argument
            ;;
        -z|--zebedee_root)
            ZEBEDEE_ROOT="$2"
            shift # past argument
            ;;
        --test)
            testing=1
            ;;
        *)
            log Bad arg: $key >&2
            exit 2 # unknown option
            ;;
    esac
    shift # past argument or value
done
log "Collection = $collectionName"
log "DIRECTORY  = $DIRECTORY"
log "Token      = $TOKEN"

# traverse collections
filesToSend=$(find ${DIRECTORY} -type f)

# Create collection
log "Creating Collection : $collectionName"

if [[ -z $testing ]]; then
  response=$(curl -sb -X  POST --cookie "access_token=${TOKEN}" \
        $HOST"/zebedee/collection" -d "$collectionJson" -k)
fi

log Response: $response
# Find the collectionID in the json message. This is needed to add content
# to the collection. (We also need to remove the quotes from the string)
collectionID=$(echo $response | jq '.id')
collectionID="${collectionID%\"}"
collectionID="${collectionID#\"}"

# Add content to the collection
running=0
run_seq=0
error_count=0
for file in ${filesToSend[@]}; do
  uri=${file/${DIRECTORY}/}
  curlUri=$HOST"/zebedee/content/"$collectionID"?uri="$uri"&overwriteExisting=true&recursive=undefined"
  if [[ $string != *".json" ]]; then
    curlUri=$curlUri"&validateJson=false"
  fi
  if (( running >= max_running )); then
    wait -n
    res=$?
    let running--
    [[ $res != 0 ]] && let error_count++
    if [[ $error_count -ge $max_errors ]]; then
      log FATAL Aborting after $error_count errors: $res
      break
    fi
  fi
  {
    if [[ -z $testing ]]; then
      fileAdded=$(curl -sb -X POST --cookie "access_token=${TOKEN}" $curlUri -d @$file -k)
    else
      sleep 1.$run_seq
      fileAdded=true
      if [[ $run_seq == *[248] ]]; then
        fileAdded=$run_seq  # simulate failure for some jobs
      fi
    fi
    if [[ "$fileAdded" != "true" ]]; then
      log "ERROR [$run_seq] Failed to add $uri to collection: $fileAdded"
      exit 2
    fi
    log "[$run_seq] Added $uri to collection"
  } &
  let running++
  let run_seq++
done
while (( running > 0 )); do
  wait -n; res=$?
  [[ $res != 0 ]] && let error_count++
  let running--
done

if [[ $error_count -gt 0 ]]; then
  log "FATAL Collection had $error_count errors - aborting"
  exit $error_count
fi

inprogressDirectory=${ZEBEDEE_ROOT}"/zebedee/collections/"${collectionID%-*}"/inprogress/*"
completeDirectory=${ZEBEDEE_ROOT}"/zebedee/collections/"${collectionID%-*}"/reviewed"
if [[ -z $testing ]]; then
  log "Moving content to complete $completeDirectory"
  mv $inprogressDirectory $completeDirectory
fi
log "Collection ready to be approved - $error_count errors"
