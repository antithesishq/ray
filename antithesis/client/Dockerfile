FROM ubuntu:22.04
RUN apt-get update
RUN apt-get install -y python-is-python3 python3-pip
# FROM docker.io/rayproject/ray:2.48.0
RUN pip install ray[client] antithesis
COPY --chmod=777 entrypoint.sh /ray-test/
COPY --chmod=777 healthcheck.py /ray-test/
# COPY --chmod=777 singleton_driver_monte_carlo.py /opt/antithesis/test/v1/integration/
COPY --chmod=777 singleton_driver_actor_task.py /opt/antithesis/test/v1/integration/
WORKDIR /ray-test
CMD ./entrypoint.sh
