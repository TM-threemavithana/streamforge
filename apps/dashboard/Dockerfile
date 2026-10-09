# syntax=docker/dockerfile:1
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci || npm install

COPY . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:alpine

USER root
RUN addgroup -g 10001 -S streamforge 2>/dev/null || true && \
    adduser -u 10001 -S streamforge -G streamforge 2>/dev/null || true

COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf.template
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && \
    mkdir -p /tmp/client_temp /tmp/proxy_temp /tmp/fastcgi_temp /tmp/uwsgi_temp /tmp/scgi_temp && \
    chown -R 10001:10001 /tmp /usr/share/nginx/html /entrypoint.sh /etc/nginx

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
