FROM python:3-onbuild
MAINTAINER juriad@gmail.com

RUN mkdir /var/log/loader/

# Run the command on container startup
CMD ["python", "./loader.py", "./loader.ini"]
