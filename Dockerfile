FROM python:3.11.1

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "/app/entrypoint.sh"]

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR /app

COPY pyproject.toml .
COPY poetry.lock .

RUN $HOME/.local/bin/poetry install

COPY . .

