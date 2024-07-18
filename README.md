## Airflow

```
# For macOS increse memory limit

docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'

# Remove

docker compose down --volumes --remove-orphans
```
