FROM python:3.8-buster

## System packages
RUN apt-get update &&\
    yes | apt-get install git python3-dev unixodbc-dev g++  \
    apt-utils vim unixodbc odbc-postgresql awscli wkhtmltopdf \
    fonts-font-awesome

## External packages
WORKDIR /opt
#Creating PDFs from HTML
RUN wget https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.3/wkhtmltox-0.12.3_linux-generic-amd64.tar.xz --no-check-certificate && \
    tar -vxf wkhtmltox-0.12.3_linux-generic-amd64.tar.xz && \
    rm -rf wkhtmltox-0.12.3_linux-generic-amd64.tar.xz

## Databases, ODBC
# Redshift
RUN wget https://redshift-downloads.s3.amazonaws.com/drivers/odbc/1.4.10.1000/AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb && \
    apt install ./AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb && \
    rm -rf AmazonRedshiftODBC-64-bit-1.4.10.1000-1.x86_64.deb

WORKDIR /code
# add odbcini configs
COPY config/odbcinst.ini .
RUN echo "" >> /etc/odbcinst.ini && \
    cat odbcinst.ini >> /etc/odbcinst.ini && \
    rm -f odbcinst.ini

COPY config/requirements.txt .
RUN pip3.8 install -r requirements.txt && \
    rm -f requirements.txt

COPY docker-entrypoint.sh /code

RUN mkdir /home/analyst/
WORKDIR /home/analyst/

ENTRYPOINT ["/code/docker-entrypoint.sh"]