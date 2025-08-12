.PHONY: ingest transform elt

ingest:
	uv run python dlt/northwind/northwind.py prod

transform:
	uv run sqlmesh -p sqlmesh plan prod --run --auto-apply

elt: ingest transform