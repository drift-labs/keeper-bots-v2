FROM node:20.18.1 AS builder
RUN npm install -g husky

COPY package.json yarn.lock ./

WORKDIR /app

COPY . .
RUN yarn install
RUN node esbuild.config.js

FROM node:20.18.1-alpine
# 'bigint-buffer' native lib for performance
RUN apk add python3 make g++ --virtual .build &&\
    npm install -C /lib bigint-buffer &&\
    apk del .build
COPY --from=builder /app/lib/ ./lib/

EXPOSE 9464

CMD ["node", "./lib/index.js"]
