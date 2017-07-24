#!/bin/bash
$TRAVIS_BUILD_DIR/travis/install_gcloud.sh app-engine-python app-engine-go
echo $sandbox_service_key | base64 --decode -i > ${HOME}/gcloud-service-key.json
gcloud auth activate-service-account --key-file ${HOME}/gcloud-service-key.json
ls /home/travis/google-cloud-sdk/ 
ls /home/travis/
export GOOGLE_APPLICATION_CREDENTIALS="${HOME}/gcloud-service-key.json" && export APPENGINE_DEV_APPSERVER="${HOME}/google-cloud-sdk/bin/dev_appserver.py" && source "${HOME}/google-cloud-sdk/path.bash.inc" && go test -v ./...

