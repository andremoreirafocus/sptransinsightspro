FROM postgres:16

# Install the build dependencies and pg_partman
RUN apt-get update && apt-get install -y \
    postgresql-16-partman \
    && rm -rf /var/lib/apt/lists/*

    