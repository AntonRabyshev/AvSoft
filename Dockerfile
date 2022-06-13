FROM python:3.7.1-slim-stretch

RUN mkdir /av_docker
RUN mkdir /dir1
RUN mkdir /dir2

COPY requirements.txt /av_docker/

RUN python -m pip install -r /av_docker/requirements.txt

COPY .env /av_docker/
COPY error_handler.py /av_docker/
COPY file_generator.py /av_docker/
COPY mylogger.py /av_docker/
COPY utils.py /av_docker/
COPY sender.py /av_docker/
COPY parser.py /av_docker/
COPY reader.py /av_docker/
COPY wrapper.sh /av_docker/



