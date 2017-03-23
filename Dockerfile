FROM qa.stratio.com/mesosphere/spark:1.0.7-2.1.0-hadoop-2.7

ARG VERSION

COPY dist /opt/sds/dist

RUN mv /opt/spark/dist /opt/spark/bak && \
    mv /opt/sds/dist /opt/spark/dist && \
    cp /opt/spark/bak/conf/* /opt/spark/dist/conf && \
    chmod -R 777 /opt/spark/dist/ && \
    rm -rf /opt/spark/bak

CMD [""]

