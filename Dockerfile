###
# vert.x docker device-manager using a Java verticle packaged as a fatjar
# To build:
#  docker build -t fleettracker/ft-gps-service .
# To run:
#   docker run -t -i -p 8080:8080 fleettracker/ft-gps-service
###

FROM java:8

EXPOSE 8080

# Copy your fat jar to the container
ADD build/distributions/ft-gps-service-3.1.0.tar /ft-gps-service

# Launch the verticle
ENV WORKDIR /ft-gps-service
ENTRYPOINT ["sh", "-c"]
CMD ["cd $WORKDIR ; ./gps-service.sh"]
