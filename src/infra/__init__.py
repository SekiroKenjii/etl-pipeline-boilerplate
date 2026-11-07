"""Adapters to external systems, implementing the ports of `core.contract`.

They know their technology (file systems, GCS, httpx...) but nothing of ETL.
Each family may need an optional dependency, installed as an extra:
`pip install etl-pipeline-boilerplate[http]`. `core` never imports `infra`.
"""
