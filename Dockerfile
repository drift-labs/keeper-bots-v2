FROM public.ecr.aws/docker/library/node:22.14 AS builder

COPY package.json yarn.lock ./

WORKDIR /app

COPY . .

WORKDIR /app/drift-common/protocol/sdk
RUN yarn install
RUN yarn run build

WORKDIR /app/drift-common/common-ts
RUN yarn install
RUN yarn run build

WORKDIR /app
RUN yarn install
RUN node esbuild.config.js

FROM public.ecr.aws/docker/library/node:22.14.0-alpine
# 'bigint-buffer' native lib for performance
RUN apk add python3 make g++ --virtual .build &&\
    npm install -C /lib bigint-buffer @triton-one/yellowstone-grpc@1.3.0 helius-laserstream rpc-websockets@7.11.2 &&\
    apk del .build &&\
    rm -rf /root/.cache/ /root/.npm /usr/local/lib/node_modules 
COPY --from=builder /app/lib/ ./lib/

EXPOSE 9464

CMD ["node", "./lib/index.js"]
