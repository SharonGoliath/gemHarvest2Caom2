FROM bucket.canfar.net/cadc/base-caom2-proxy
#COPY ./requirements.txt /app/
#WORKDIR /app
#RUN pip install -r requirements.txt
#COPY . /app
#ENTRYPOINT [“python”]
#CMD [“app.py”]

# FROM python:3.6

# RUN pip install flask && pip install flask_restful
# RUN pip install cadcutils && pip install caom2

RUN pip install pytest && pip install bs4

WORKDIR /app

COPY . /app
ENTRYPOINT ["python"]
CMD ["app.py"]
