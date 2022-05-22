FROM python:3.7

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --trusted-host pypi.python.org -r requirements.txt

COPY ./src /app/src
COPY ./utils/python/ /app/src
COPY ./templates/ /app/templates
COPY ./static/ /app/static
COPY ./configuration/ /app/configuration

RUN bash -c 'mkdir -p /app/errors'

EXPOSE 8090

CMD ["python", "-u", "src/launch.py"]