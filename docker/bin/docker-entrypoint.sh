#!/usr/bin/dumb-init /bin/sh
set -e

# If the config directory has been mounted through -v, we chown it.
if [ "$(stat -c %u ${SNOWPLOW_CONFIG_PATH})" != "$(id -u snowplow)" ]; then
  chown -R snowplow:snowplow ${SNOWPLOW_CONFIG_PATH}
fi

# Make an artifact executable for snowplow:snowplow
chmod +x /opt/docker/bin/${SNOWPLOW_BIGQUERY_APP}

# Make sure we run the app as the snowplow user
exec gosu snowplow:snowplow /opt/docker/bin/${SNOWPLOW_BIGQUERY_APP} "$@"