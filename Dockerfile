FROM python:3
COPY . /app
WORKDIR /app
RUN pip install flask google-auth google-auth-oauthlib google-auth-httplib2 urllib3 pandas_gbq pandas
CMD ["python3", "app.py"]
