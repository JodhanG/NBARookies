FROM python:3.9.1

RUN pip install pandas sqlalchemy psycopg2 requests beautifulsoup4 lxml numpy argparse



WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]

