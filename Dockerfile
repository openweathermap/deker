# for manual build use command "owm build {your_tag}" or
  # docker build
    # --build-arg GITLAB_PYPI_TOKEN=${OWM_GIT_TOKEN}
    # --build-arg GITLAB_PYPI_TOKEN_PASS=${OWM_GIT_TOKEN_PASS}
    # --build-arg PYTHON_PACKAGE_NAME='owm name' (optional key, if not passed - your package name is used)
    # -t {your_tag} .

FROM python:3.9.0-slim-buster

# sets passed build arguments
ARG GITLAB_PYPI_TOKEN
ENV OWM_GIT_TOKEN=${GITLAB_PYPI_TOKEN}
ARG GITLAB_PYPI_TOKEN_PASS
ENV OWM_GIT_TOKEN_PASS=${GITLAB_PYPI_TOKEN_PASS}
ARG PACKAGE_NAME
ENV PYTHON_PACKAGE_NAME=${PACKAGE_NAME:-deker}

# creates application directory
RUN mkdir /PYTHON_PACKAGE_NAME
WORKDIR /$PYTHON_PACKAGE_NAME

# updates system and install dependencies
RUN apt update && apt upgrade -y && pip install --upgrade pip

# copies and installs project depedencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# cleans image cache (reduce image size)
RUN apt-get clean autoclean && apt-get autoremove --yes && rm -rf /var/lib/{apt,dpkg,cache,log}/

# copies project files and tests
COPY ./$PYTHON_PACKAGE_NAME .

CMD [ "/bin/bash" ]
