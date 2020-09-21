FROM python:3.7

WORKDIR /home/servicex-backend-tests

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .