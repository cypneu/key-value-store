FROM debian:bookworm-slim

ARG ZIG_VERSION=0.14.1
ARG ZIG_ARCH=aarch64

RUN apt-get update && apt-get install -y --no-install-recommends \
  curl xz-utils ca-certificates build-essential git gdb strace pkg-config \
  python3 python3-pytest \
  && rm -rf /var/lib/apt/lists/*

RUN curl -L "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
  -o /tmp/zig.tar.xz \
  && tar -xJf /tmp/zig.tar.xz -C /opt \
  && ln -s /opt/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}/zig /usr/local/bin/zig \
  && rm /tmp/zig.tar.xz

RUN useradd -m dev
USER dev
WORKDIR /app

ENV ZIG_GLOBAL_CACHE_DIR=/home/dev/.cache/zig \
  ZIG_LOCAL_CACHE_DIR=/home/dev/.cache/zig-local

CMD ["/bin/bash"]
