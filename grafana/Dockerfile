# ================================================================
# Grafana — Boston Rideshare Real-Time Operations Dashboard
# Extends official Grafana OSS with BigQuery auto-provisioning
# ================================================================
FROM grafana/grafana-oss:11.1.0

USER root

# Install jq for service account JSON parsing in entrypoint
RUN apk add --no-cache jq

# Ensure provisioning directories are writable by grafana user
RUN chown -R grafana:root /etc/grafana/provisioning

# Copy custom entrypoint that auto-configures BigQuery datasource
COPY --chmod=755 entrypoint.sh /custom-entrypoint.sh

# Copy dashboard provisioning config
COPY provisioning/dashboards/dashboards.yml /etc/grafana/provisioning/dashboards/dashboards.yml

# Copy dashboard JSON files
COPY dashboards/ /var/lib/grafana/dashboards/

USER grafana

ENTRYPOINT ["/custom-entrypoint.sh"]
