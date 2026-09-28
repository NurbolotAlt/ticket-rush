FROM nginx:alpine

# openssl — для self-signed сертификата при старте контейнера.
RUN apk add --no-cache openssl

# Сертификат генерируется при СТАРТЕ контейнера (скрипты из /docker-entrypoint.d/
# официальный образ выполняет перед запуском nginx), а не при сборке: иначе
# закрытый ключ был бы зашит в слой образа. sed убирает CRLF на случай Windows.
COPY nginx/generate-cert.sh /docker-entrypoint.d/05-generate-cert.sh
RUN sed -i 's/\r$//' /docker-entrypoint.d/05-generate-cert.sh \
    && chmod +x /docker-entrypoint.d/05-generate-cert.sh

COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Пути — от КОРНЯ проекта (build context = корень): Dockerfile копирует и nginx/,
# и static/, а COPY не видит файлов за пределами контекста.
COPY static /usr/share/nginx/html

EXPOSE 80 443

CMD ["nginx", "-g", "daemon off;"]
