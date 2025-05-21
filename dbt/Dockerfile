FROM python:3.9-slim

WORKDIR /usr/app/dbt

# Install dbt-core, PostgreSQL adapter, and pytz
RUN pip install --upgrade pip && \
    pip install dbt-postgres==1.3.0 pytz

# Set dbt profiles directory
ENV DBT_PROFILES_DIR="/usr/app/dbt"

# Keep container running
CMD ["tail", "-f", "/dev/null"]