# 96bulls services image: one image, SERVICE= picks the entry point.
# The pipeline framework and object store are workspace packages in this repo
# (packages/pipeline, packages/object-store); no vendored sources are needed.
FROM oven/bun:1.4.2-slim AS base
WORKDIR /app
COPY package.json bun.lock tsconfig.json LICENSE NOTICE ./
COPY packages/pipeline/package.json packages/pipeline/
COPY packages/object-store/package.json packages/object-store/
RUN bun install --frozen-lockfile --ignore-scripts --no-progress
COPY packages packages
RUN bun run --filter @relayhistory/object-store build
COPY supabase supabase
COPY certs certs
COPY src src
COPY docker/entrypoint.sh /entrypoint.sh
# Dev dependencies (tsc) are needed for the object-store build above; production mode starts here.
ENV NODE_ENV=production
RUN chmod +x /entrypoint.sh && mkdir -p /telemetry /concourse && chown -R bun:bun /app /telemetry /concourse
USER bun
ENV OTEL_EXPORTER_OTLP_ENDPOINT=""
ENTRYPOINT ["/entrypoint.sh"]
