# Imagen oficial de Metabase
FROM metabase/metabase:latest

# Metabase escuchará en 10000
ENV MB_JETTY_PORT=10000
EXPOSE 10000

# (opcional para que guarde DB embebida)
ENV MB_DB_FILE=/data/metabase.db
