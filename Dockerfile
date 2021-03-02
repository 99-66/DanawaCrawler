FROM python:3.6.6-slim

COPY ./ /danawaCrawling
WORKDIR /danawaCrawling

RUN pip install -r requirements.txt

CMD ["/bin/sh"]