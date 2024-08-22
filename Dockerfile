FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:20240628-rc00 AS template_launcher
# TODO update this image, there's a known gcs read bug, but it doesn't impact us currently
FROM apache/beam_python3.10_sdk:2.58.1 

ARG WORKDIR=/pipeline
WORKDIR ${WORKDIR}

# Copy Flex Template launcher binary from the launcher image to use this image as a Flex Template image
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get update && \
    apt-get install -y libeccodes0 && \
    rm -rf /var/lib/apt/lists/*

# Copy in source code last so code changes don't invalidate cache for slower dependency install steps
COPY . . 
RUN pip install -e .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"