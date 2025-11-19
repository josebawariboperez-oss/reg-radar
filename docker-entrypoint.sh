#!/bin/sh
set -e

# Render pone el puerto en $PORT; Metabase usa MB_JETTY_PORT numérico
if [ -n "$PORT" ]; then
  export MB_JETTY_PORT="$PORT"
else
  export MB_JETTY_PORT="3000"
fi

# Arrancar Metabase
exec java -XX:+UseSerialGC -Xmx384m -jar /app/metabase.jar