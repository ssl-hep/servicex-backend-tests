FROM python:3.7

WORKDIR /home/servicex-backend-tests

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ARG user_name
ARG pass_word

ENV sx_username = $user_name
ENV sx_password = $pass_word

RUN printf "api_endpoint:\n" > ./.servicex
RUN printf "email: " > ./.servicex
RUN printf $sx_username > ./.servicex
RUN printf "\n" > ./.servicex
RUN printf "password: " > ./.servicex
RUN printf $sx_password > ./.servicex

COPY tests ./tests