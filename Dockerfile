FROM python:latest

COPY twitter_source.py .
RUN pip install twitter
RUN pip install kafka
CMD ["python", "twitter_source.py"]
