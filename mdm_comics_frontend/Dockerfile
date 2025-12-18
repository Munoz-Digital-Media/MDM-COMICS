# Frontend build - serves static React app
# GOVERNANCE: constitution_cyberSec.json Section 5 - Zero cleartext
FROM node:20-alpine AS builder

WORKDIR /app

# Vite env vars must be available at build time
# CRITICAL: These MUST be HTTPS URLs for production
ARG VITE_API_URL=https://mdm-comics-backend-development.up.railway.app/api
ARG VITE_MIDDLEWARE_URL=https://mdm-comics-middleware-development.up.railway.app

# Force cache invalidation on config changes
ARG CACHE_BUSTER=v3-centralized-config-20241218

# Validate and enforce HTTPS at build time
RUN echo "=== BUILD CONFIG VALIDATION ===" && \
    echo "Raw VITE_API_URL: $VITE_API_URL" && \
    echo "Raw VITE_MIDDLEWARE_URL: $VITE_MIDDLEWARE_URL" && \
    echo "=== ENFORCING HTTPS ===" && \
    if echo "$VITE_API_URL" | grep -q "^http://[^l]"; then \
      echo "WARNING: Converting HTTP to HTTPS for VITE_API_URL"; \
    fi && \
    echo "=== VALIDATION COMPLETE ==="

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# Build with HTTPS-enforced URLs
# Double-enforcement: sed converts any HTTP to HTTPS (except localhost)
RUN export VITE_API_URL=$(echo "${VITE_API_URL}" | sed 's|^http://\([^l]\)|https://\1|') && \
    export VITE_MIDDLEWARE_URL=$(echo "${VITE_MIDDLEWARE_URL}" | sed 's|^http://\([^l]\)|https://\1|') && \
    echo "=== BUILDING WITH ===" && \
    echo "VITE_API_URL=$VITE_API_URL" && \
    echo "VITE_MIDDLEWARE_URL=$VITE_MIDDLEWARE_URL" && \
    npm run build

# Verify build output doesn't contain HTTP URLs to production domains
RUN echo "=== VERIFYING BUILD OUTPUT ===" && \
    if grep -r "http://mdm-comics" dist/ 2>/dev/null | grep -v "localhost"; then \
      echo "ERROR: Found HTTP URL to production domain in build output!"; \
      exit 1; \
    fi && \
    echo "Build verification PASSED: No HTTP URLs to production domains"

# Production runtime stage
FROM node:20-alpine

WORKDIR /app

RUN npm install -g serve

COPY --from=builder /app/dist ./dist

ENV HOST=0.0.0.0
ENV PORT=3000
EXPOSE 3000

CMD ["sh", "-c", "serve -s ./dist -l tcp://0.0.0.0:${PORT}"]
