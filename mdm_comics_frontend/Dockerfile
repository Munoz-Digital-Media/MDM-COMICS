# Frontend build - serves static React app
FROM node:20-alpine AS builder

WORKDIR /app

# Vite env vars must be available at build time
ARG VITE_API_URL=https://mdm-comics-backend-development.up.railway.app/api
ARG VITE_MIDDLEWARE_URL=https://mdm-comics-middleware-development.up.railway.app

# Debug: Print received build args
RUN echo "=== BUILD ARGS RECEIVED ===" && \
    echo "VITE_API_URL=$VITE_API_URL" && \
    echo "VITE_MIDDLEWARE_URL=$VITE_MIDDLEWARE_URL" && \
    echo "==========================="

ARG CACHE_BUSTER=https-hardcode-20241218
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
# Ensure HTTPS at build time (sed as extra safety)
RUN VITE_API_URL=$(echo "$VITE_API_URL" | sed 's|^http://|https://|') \
    VITE_MIDDLEWARE_URL=$(echo "$VITE_MIDDLEWARE_URL" | sed 's|^http://|https://|') \
    sh -c 'echo "Building with VITE_API_URL=$VITE_API_URL" && npm run build'

FROM node:20-alpine

WORKDIR /app

RUN npm install -g serve

COPY --from=builder /app/dist ./dist

ENV HOST=0.0.0.0
ENV PORT=3000
EXPOSE 3000

CMD ["sh", "-c", "serve -s ./dist -l tcp://0.0.0.0:${PORT}"]
