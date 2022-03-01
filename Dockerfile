FROM python:3.9.10

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "/app/entrypoint.sh"]

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
ENV PATH "${PATH}:/root/.poetry/bin"

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry install

COPY . .

