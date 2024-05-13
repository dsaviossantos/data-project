FROM python:3.10.8-slim-buster

ENV DEBIAN_FRONTEND=noninteractive

COPY pipeline /pipeline

WORKDIR /intelligentia/integrations

RUN apt update \
    &&  apt install -y \
        gcc \
        telnet \
        curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry \
    && poetry self update \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi \
    && apt remove -y gcc \
    && rm -rf /var/lib/apt \
    && rm -rf /var/lib/dpkg \
    && rm -rf /var/cache/* \
    && rm -rf /var/log/* \
    && rm -rf /usr/share/doc/* \
    && rm -rf /usr/share/info/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /root/.cache \
    && rm -rf /tmp/*
