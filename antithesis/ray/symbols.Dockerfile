FROM d7b55bdbddfd

USER root
RUN mkdir /symbols
RUN ln -s /home/ray/ray/python/ray/core/src/ray/raylet/raylet /symbols/raylet
RUN ln -s /home/ray/ray/python/ray/_raylet.so /symbols/_raylet.so
RUN ln -s /home/ray/ray/python/ray/core/libjemalloc.so /symbols/libjemalloc.so
USER ray
