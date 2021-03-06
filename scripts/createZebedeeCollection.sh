#!/usr/bin/env bash

# Notes : This needs jq installed use `brew install jq`

log()  { echo $(date '+%Y-%m-%d %T') "$@"; }
warn() { log "$@" >&2 ; }
die()  { local rc=$1; shift; warn "$@"; exit $rc; }
testing=
faily=
max_running=50
max_errors=1
plaintext=
ZEBEDEE_ROOT=${ZEBEDEE_ROOT:-$zebedee_root}
DIRECTORY=.
HOST=${HOST:-http://localhost:8081}

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
        --auth)
            auth_info_login=${2%%:*} # email:password get email
            auth_info_passw=${2##*:} # email:password get password
            log "u=[$auth_info_login] p=[$auth_info_passw]"
            shift # past argument
            ;;
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
        -P|--plaintext)
            plaintext=1
            ;;
        --test)
            testing=1
            ;;
        --faily)
            faily=1
            ;;
        *)
            die 2 Bad arg: $key
            ;;
    esac
    shift # past argument or value
done

if [[ -z $TOKEN ]]; then
  # no token arg, so login to get token... then remove leading/trailing double-quote
  [[ -z $auth_info_login ]] && die 2 "Bad auth details - need --auth user:pass?"
  TOKEN=$(curl -sH 'Content-type: application/json' -d '{"email":"'"$auth_info_login"'","password":"'"$auth_info_passw"'"}' $HOST/zebedee/login)
  TOKEN=${TOKEN%\"}
  TOKEN=${TOKEN#\"}
fi
[[ -z $TOKEN ]] && die 2 "Bad token - auth problem?"

log "Collection   = $collectionName"
log "DIRECTORY    = $DIRECTORY"
log "HOST         = $HOST"
log "Token        = $TOKEN"

# traverse collections
filesToSend=$(find ${DIRECTORY} -type f)

# Create collection
log "Creating     : $collectionName"

if [[ -z $testing ]]; then
  response=$(curl -sb -X  POST --cookie "access_token=${TOKEN}" \
        $HOST"/zebedee/collection" -d "$collectionJson" -k)
fi

log "Response     : $response"
# Find the collectionID in the json message. This is needed to add content
# to the collection.
collectionID=$(echo $response | jq -r '.id')
log "collectionID : $collectionID"
if [[ $collectionID == null || -z $collectionID ]]; then
  die 2 Bad collectionID
fi

collectionDirectory=$ZEBEDEE_ROOT/zebedee/collections/${collectionID%-*}
if [[ ! -f $collectionDirectory.json ]]; then
  die 2 Cannot see $collectionDirectory.json
fi
if [[ -n $plaintext ]]; then
  backupExt=.create$$
  sed -i $backupExt -e 's/"isEncrypted":true,/"isEncrypted":false,/' $collectionDirectory.json || exit 2
  rm $collectionDirectory.json$backupExt
fi

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
      warn FATAL Aborting after $error_count errors: $res
      break
    fi
  fi
  {
    if [[ -z $testing ]]; then
      fileAdded=$(curl -sb -X POST --cookie "access_token=${TOKEN}" $curlUri -d @$file -k)
    else
      log send $curlUri
      sleep 0.$run_seq
      fileAdded=true
      if [[ -n $faily && $run_seq == *[248] ]]; then
        fileAdded=$run_seq  # simulate failure for some jobs
      fi
    fi
    if [[ "$fileAdded" != "true" ]]; then
      die 2 "ERROR [$run_seq] Failed to add $uri to collection: $fileAdded"
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
  die $error_count "FATAL Collection had $error_count errors - aborting"
fi

inprogressDirectory=${collectionDirectory}/inprogress
completeDirectory=${collectionDirectory}/reviewed
if [[ -z $testing ]]; then
  log "Moving content to complete $completeDirectory"
  mv $inprogressDirectory/* $completeDirectory || exit 2
fi
log "Collection ready to be approved - $error_count errors"
