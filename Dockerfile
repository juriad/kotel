FROM python:3.11-alpine
MAINTAINER juriad@gmail.com

WORKDIR /app

RUN apk add --no-cache curl-dev libcurl curl gcc
ENV PYCURL_SSL_LIBRARY=openssl

RUN apk add --no-cache --virtual .build-deps build-base curl-dev

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN apk del --no-cache --purge .build-deps \
    && rm -rf /var/cache/apk/*

RUN mkdir /var/log/loader/

COPY . .

# Run the command on container startup
CMD ["python3", "./loader.py", "./loader.ini"]
