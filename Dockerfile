### Test NiFi Build with custom processor loaded ###

FROM apache/nifi:1.9.2

ARG VERSION=1.0
### Copy the latest Build 
COPY ./encodings/nifi-encodings-nar/target/nifi-encodings-nar-${VERSION}.nar /opt/nifi/nifi-current/lib/




### SCRIPT FROM APACHE/NIFI ###


# Clear nifi-env.sh in favour of configuring all environment variables in the Dockerfile
RUN echo "#!/bin/sh\n" > $NIFI_HOME/bin/nifi-env.sh

# Web HTTP(s) & Socket Site-to-Site Ports
EXPOSE 8080 8443 10000

WORKDIR ${NIFI_HOME}

# Apply configuration and start NiFi
#
# We need to use the exec form to avoid running our command in a subshell and omitting signals,
# thus being unable to shut down gracefully:
# https://docs.docker.com/engine/reference/builder/#entrypoint
#
# Also we need to use relative path, because the exec form does not invoke a command shell,
# thus normal shell processing does not happen:
# https://docs.docker.com/engine/reference/builder/#exec-form-entrypoint-example
ENTRYPOINT ["../scripts/start.sh"]