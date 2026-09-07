#!/bin/bash
cd /opt/Linkx_xmaintenance
sudo PYTHONPATH=/opt/Linkx_xmaintenance/src /opt/Linkx_xmaintenance/.venv/bin/python -m linkx_xvigilance.runner --once
