# Separate Node24 image; the Bun services image does not select this runtime.
FROM oven/bun:1.4.2-slim AS build
WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
COPY packages/pipeline/package.json packages/pipeline/
COPY packages/object-store/package.json packages/object-store/
RUN bun install --frozen-lockfile --ignore-scripts --no-progress
COPY packages packages
COPY src src
# Build inputs only: the derived-schema plugin embeds the migration generation and verifier
# SQL into the bundle (as the Lambda build does). supabase/ never reaches the runtime image.
COPY supabase supabase
COPY stacks/aws/derived-schema-build.ts stacks/aws/
COPY scripts/build-large-read-worker.ts scripts/
RUN bun scripts/build-large-read-worker.ts /out/large-read-main.mjs
FROM node:24.21.0-bookworm-slim
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=$SOURCE_REVISION
WORKDIR /app
COPY LICENSE NOTICE ./
# Self-contained bundle (pg included); no node_modules or supabase/ in the runtime image.
COPY --from=build /out/large-read-main.mjs ./large-read-main.mjs
ENV NODE_ENV=production
USER node
# Heap ceiling from docs/canonical-large-worker-runtime.md (measured 8.6 GB RSS > Node's default heap).
CMD ["node", "--max-old-space-size=49152", "/app/large-read-main.mjs"]
