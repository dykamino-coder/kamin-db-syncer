# ---- build ----
FROM golang:1.27-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kamin-db-syncer .

# ---- runtime ----
FROM debian:trixie-slim
# Microsoft publishes SqlPackage for Linux only behind this evergreen link
ARG SQLPACKAGE_URL=https://aka.ms/sqlpackage-linux

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates curl unzip git openssh-client tzdata libicu76 \
 && curl -fsSL -o /tmp/sqlpackage.zip "$SQLPACKAGE_URL" \
 && mkdir -p /opt/sqlpackage && unzip -q /tmp/sqlpackage.zip -d /opt/sqlpackage \
 && chmod +x /opt/sqlpackage/sqlpackage && ln -s /opt/sqlpackage/sqlpackage /usr/local/bin/sqlpackage \
 && rm /tmp/sqlpackage.zip \
 && apt-get purge -y curl unzip && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home kamin && mkdir -p /work && chown kamin /work
COPY --from=build /out/kamin-db-syncer /usr/local/bin/kamin-db-syncer

USER 10001
WORKDIR /work
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 WORK_DIR=/work
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kamin-db-syncer"]
