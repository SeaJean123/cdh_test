# syntax=docker/dockerfile:experimental
FROM docker
COPY --from=docker/buildx-bin:latest /buildx /usr/libexec/docker/cli-plugins/docker-buildx
RUN docker buildx version

FROM public.ecr.aws/sam/build-python3.9:latest

ARG requirements_filter
VOLUME /mnt
WORKDIR /tmp

RUN if [ -n "$http_proxy" ]; then \
    git clone https://github.com/bryanpkc/corkscrew.git \
    && cd corkscrew \
    && git -c advice.detachedHead=false checkout -m master \
    && autoreconf --install \
    && ./configure \
    && make \
    && make install; \
    fi

RUN mkdir ~/.ssh
RUN echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" >> ~/.ssh/known_hosts
RUN echo "|1|lEnek3SEzxjrX8bvsIrijZtSuPo=|RkADVNnFxN0yl2fN8aAxv3BmCqQ= ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC1mMeortEsruBLnRKltGfQUjA4lghYZWMljdeTdE5Tx86Mrj7WGZVjkNzJYDnoirU8Nb79TbHuYu84mLglqRB/z/2L1K32eCUKR+LipLvqFZvhtTSxlO80UIBBRxyjBwMEpdmfJUqZJCcYw5X+jVADYYfX93/gD92MNzWp6D8tW4fvnWlAaMIKhUxA/k4iXvwA77VpmvIB7Twxt8NHsa4ehER+JzSuZoOIqUARzkhvBuHTRlxTlMuyVGfyOYfixntZ1+BiQsh9r/HxUvAnvwXj5Jb67hPkATKPAibo2tyNCVfoJernlVCrWNEyW1StNboPjxJLeKEzFWbHR6fOa+FB" >> ~/.ssh/known_hosts
RUN echo "|1|MTgE2Upd5vI1GwlaYvzS2aFxckc=|EJCvx+d0luwhqmyF+LQAxeQtjRU= ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC1mMeortEsruBLnRKltGfQUjA4lghYZWMljdeTdE5Tx86Mrj7WGZVjkNzJYDnoirU8Nb79TbHuYu84mLglqRB/z/2L1K32eCUKR+LipLvqFZvhtTSxlO80UIBBRxyjBwMEpdmfJUqZJCcYw5X+jVADYYfX93/gD92MNzWp6D8tW4fvnWlAaMIKhUxA/k4iXvwA77VpmvIB7Twxt8NHsa4ehER+JzSuZoOIqUARzkhvBuHTRlxTlMuyVGfyOYfixntZ1+BiQsh9r/HxUvAnvwXj5Jb67hPkATKPAibo2tyNCVfoJernlVCrWNEyW1StNboPjxJLeKEzFWbHR6fOa+FB" >> ~/.ssh/known_hosts
RUN if [ -n "$http_proxy" ]; then \
    export proxy_host=$(echo "$http_proxy" | sed 's/http:\/\/\(.*\):.*/\1/') \
    && export proxy_port=$(echo "$http_proxy" | sed 's/http:\/\/.*:\(.*\)/\1/' | tr -d "/") \
    && echo -e "Host github.com\n  ProxyCommand corkscrew $proxy_host $proxy_port %h %p" > ~/.ssh/config; fi

COPY requirements.txt /tmp/

RUN grep -Ev "$requirements_filter" requirements.txt > pypi_requirements.txt || true
RUN grep -E "$requirements_filter" requirements.txt > git_requirements.txt || true

RUN python -m pip install -r pypi_requirements.txt --target /tmp/python/
RUN --mount=type=ssh python -m pip install -r git_requirements.txt --target /tmp/python/

RUN python -m compileall -b /tmp/python/
RUN find /tmp/python/ -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
RUN find /tmp/python/ -type f -a -name '*.py' -print0 | xargs -0 rm -f
RUN zip -X -q -r1 /tmp/dependencies.zip python
RUN chmod 666 /tmp/dependencies.zip

ENTRYPOINT ["mv", "/tmp/dependencies.zip", "/mnt/dependencies.zip"]
