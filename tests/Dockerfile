FROM python:slim-trixie

RUN pip install pytest psycopg2-binary

WORKDIR /tests
COPY tests/src .

CMD [ "python", "-m", "pytest", "-s", "-vv" ]
