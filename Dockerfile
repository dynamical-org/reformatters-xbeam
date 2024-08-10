FROM apache/beam_python3.10_sdk:2.57.0

ARG WORKDIR=/pipeline
WORKDIR ${WORKDIR}

# Copy Flex Template launcher binary from the launcher image, which makes it possible to use the image as a Flex Template base image.
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20240628-rc00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get update && \
    apt-get install -y libeccodes0 && \
    rm -rf /var/lib/apt/lists/*

COPY . .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/gefs-forecast.py"