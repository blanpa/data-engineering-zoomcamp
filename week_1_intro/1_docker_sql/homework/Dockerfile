FROM python:3.9.1

RUN mkdir /app
WORKDIR /app

RUN apt-get install wget
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

WORKDIR /app

#COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py"]