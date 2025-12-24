# Frontend build with API proxy
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# Production - Express server with API proxy
FROM node:20-alpine

WORKDIR /app

# Copy package files and install production deps only
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy built assets and server
COPY --from=builder /app/dist ./dist
COPY server.js ./

ENV PORT=3000
# Default for local development; override per-environment via Railway/ENV at runtime
ENV BACKEND_URL=https://mdm-comics-backend-development.up.railway.app

EXPOSE 3000

CMD ["node", "server.js"]
