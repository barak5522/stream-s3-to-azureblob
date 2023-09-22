FROM python:3.7-slim
COPY . /app
WORKDIR /app
RUN pip install .
CMD ["stream_s3_to_azureblob"]
