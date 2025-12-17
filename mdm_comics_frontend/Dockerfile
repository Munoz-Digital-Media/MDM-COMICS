# Frontend build - serves static React app
FROM node:20-alpine AS builder

WORKDIR /app

# Vite env vars must be available at build time
# Force HTTPS to prevent mixed content errors in production
ARG VITE_API_URL
RUN if [ -n "$VITE_API_URL" ]; then \
      export VITE_API_URL=$(echo "$VITE_API_URL" | sed 's|^http://|https://|'); \
    fi && echo "VITE_API_URL=$VITE_API_URL"
ENV VITE_API_URL=$VITE_API_URL

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
# Ensure HTTPS at build time
RUN VITE_API_URL=$(echo "$VITE_API_URL" | sed 's|^http://|https://|') npm run build

FROM node:20-alpine

WORKDIR /app

RUN npm install -g serve

COPY --from=builder /app/dist ./dist

ENV HOST=0.0.0.0
ENV PORT=3000
EXPOSE 3000

CMD ["sh", "-c", "serve -s ./dist -l tcp://0.0.0.0:${PORT}"]
