#!/bin/bash
# shellcheck disable=SC2154

# Shell file allows App engine's devappserver to override google devappserver
# and handles token issues.  This must be done in the shell file to use the
# travis path for export commands.

"${TRAVIS_BUILD_DIR}"/travis/install_gcloud.sh app-engine-python app-engine-go
if [[ -n "${sandbox_service_key}" ]]; then
  echo "${sandbox_service_key}" | base64 --decode -i > "${HOME}"/gcloud-service-key.json
  gcloud auth activate-service-account --key-file "${HOME}"/gcloud-service-key.json
fi
