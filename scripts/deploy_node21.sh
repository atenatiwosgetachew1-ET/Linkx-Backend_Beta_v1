#!/bin/bash
echo "Deploying batch_manager and linkx_worker..."
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-worker/src/batch_manager/. /opt/linkx-worker/src/batch_manager/
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-worker/src/linkx_worker/. /opt/linkx-worker/src/linkx_worker/
echo "Restarting services..."
sudo systemctl restart linkx-xvigilance-consumer
sudo systemctl restart linkx-worker
echo "Deployment successful."
