FROM apache/beam_python3.9_sdk:2.48.0

# Make the working directory in the container and copy all
ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}
COPY . ${WORKDIR}/

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/dataflow.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

# Install apache-beam and other dependencies to launch the pipeline
RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip \
    && pip install 'apache-beam[gcp]==2.48.0' \
    && pip install -U -r ${WORKDIR}/requirements.txt

RUN python setup.py install
ENV PIP_NO_DEPS=True

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]



