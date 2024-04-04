FROM redis

RUN apt-get -y update && apt-get install -y vim
RUN apt install -y python3 && apt-get install -y pip

COPY ./running /running

RUN pip install -r /running/requirements.txt --break-system-packages