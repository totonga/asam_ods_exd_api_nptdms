# GitHub: ghcr.io/<repository_owner>/asam-ods-exd-api-nptdms:latest
# docker build -t ghcr.io/totonga/asam-ods-exd-api-nptdms:latest .
# docker run --rm -it -v "$(pwd)/data":"$(pwd)/data" -p 50051:50051 ghcr.io/totonga/asam-ods-exd-api-nptdms:latest
FROM python:3.12-slim
LABEL org.opencontainers.image.source=https://github.com/totonga/asam_ods_exd_api_nptdms
LABEL org.opencontainers.image.description="ASAM ODS External Data API for National Instruments TDMS files (*.tdms)"
LABEL org.opencontainers.image.licenses=MIT
WORKDIR /app
# Create a non-root user and change ownership of /app
RUN useradd -ms /bin/bash appuser && chown -R appuser /app
# Copy source code first (needed for pip install)
COPY pyproject.toml .
# Install required packages
RUN pip3 install --upgrade pip && pip3 install .
COPY external_data_file.py ./
USER appuser
# Start server
CMD [ "python3", "external_data_file.py"]