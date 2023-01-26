FROM python:3.11
ARG user=
ARG password=
ARG host=
ARG port=
ARG database=
ARG file_url=
ARG table=
ENV user=$user password=$password host=$host port=$port database=$database file_url=$file_url table=$table
LABEL Name=ingest Version=0.0.1
COPY ingest.py requirements.txt ./
RUN apt-get update && apt-get install wget gzip
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT python ingest.py --user "$user" --password "$password" --host "$host" --port "$port" --database "$database" --file_url "$file_url" --table "$table"