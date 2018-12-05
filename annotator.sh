#!/bin/bash

# Shell file allows App engine's devappserver to override google devappserver
# and handles token issues.  This must be done in the shell file to use the
# travis path for export commands.

"${TRAVIS_BUILD_DIR}"/travis/install_gcloud.sh app-engine-python app-engine-go

# $sandbox_service_key is defined outside of this shell script, so use a pragma
# to disable the shellcheck warning about undefined lower-case variables.
# shellcheck disable=SC2154
if [[ -n "${sandbox_service_key}" ]]; then
  echo "${sandbox_service_key}" | base64 --decode -i > "${HOME}"/gcloud-service-key.json
  gcloud auth activate-service-account --key-file "${HOME}"/gcloud-service-key.json
fi
