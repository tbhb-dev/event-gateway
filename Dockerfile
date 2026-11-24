# SPDX-License-Identifier: Apache-2.0
# Copyright Tony Burns

FROM golang:1.27.4-bookworm@sha256:ce59b41415e2524ae99926fe1f37ce48ba8c194dcbd2a56a65436cdcefa2bd34 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/event-gateway ./cmd/event-gateway && \
    install -d -m 0700 /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/event-gateway /event-gateway
# A named volume mounted at /data inherits this ownership, so the server can
# write its JetStream store while the root filesystem stays read-only.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 CMD ["/event-gateway", "healthcheck"]
ENTRYPOINT ["/event-gateway"]
