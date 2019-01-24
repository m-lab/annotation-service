#!/bin/bash
#
# cron.sh creates the cron jobs needed for the annotation service, using the
# Cloud Schedule API to create app engine cron jobs.

set -ex
PROJECT=${1:?Please provide project}
BASEDIR="$(dirname "$0")"

"${BASEDIR}"/travis/schedule_appengine_job.sh "${PROJECT}" update_maxmind_datasets \
    --description="Load the list of dataset filenames for annotator daily at 3:00 UTC" \
	--relative-url="/cron/update_maxmind_datasets" \
    --schedule="every day 03:00" \
    --service="annotator"


"${BASEDIR}"/travis/schedule_appengine_job.sh "${PROJECT}" update_maxmind_datasets_sidestream \
    --description="Load the list of dataset filenames for sidestream annotator daily at 3:00 UTC" \
    --relative-url="/cron/update_maxmind_datasets" \
    --schedule="every day 03:00" \
	--service="annotatorss"

# Report all currently scheduled jobs.
gcloud --project "${PROJECT}" beta scheduler jobs list 2> /dev/null
