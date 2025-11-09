FROM metabase/metabase:latest
# Limitar memoria de la JVM para instancias pequeñas
ENV JAVA_TOOL_OPTIONS="-Xmx384m -XX:+UseSerialGC"
# Hacer que Metabase escuche el puerto que Render asigna en $PORT (o 3000 local)
CMD ["sh","-c","export MB_JETTY_PORT=${PORT:-3000}; exec java -jar /app/metabase.jar"]