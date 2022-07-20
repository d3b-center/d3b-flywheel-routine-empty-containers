# d3b-flywheel-routine-empty-containers

This script can be run to delete empty subject, session, and acquisition containers that were created more than 24 hours ago.

To run:

```bash
pip3 install -r requirements.txt
python3 fw_delete_empty_containers_DataView.py
```

Operation depends on one environment variable:

| Environment Key | Description |
|-----------------|-------------|
| FLYWHEEL_API_TOKEN | Your API token for Flywheel. It looks like `chop.flywheel.io:<random_alphanum>`.<br> D3b has a gsuite service account for this `flywheel@d3b.center`. |
