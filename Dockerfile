FROM public.ecr.aws/docker/library/node:24 AS builder

COPY package.json yarn.lock ./

# Enable Corepack for Yarn v4
RUN corepack enable && corepack prepare yarn@4 --activate

WORKDIR /app

COPY package.json yarn.lock ./

COPY . .

RUN yarn install --immutable
RUN node esbuild.config.js

WORKDIR /app/drift-common/common-ts
RUN yarn install
RUN yarn run build

WORKDIR /app/drift-common/protocol/sdk
RUN yarn install
RUN yarn run build

FROM public.ecr.aws/docker/library/node:24-alpine
# 'bigint-buffer' native lib for performance
RUN apk add python3 make g++ --virtual .build &&\
    npm install -C /lib bigint-buffer @triton-one/yellowstone-grpc@1.3.0 helius-laserstream rpc-websockets@7.10.0 &&\
    apk del .build &&\
    rm -rf /root/.cache/ /root/.npm /usr/local/lib/node_modules
# Create symlinks for .cjs -> .js to satisfy both import styles
RUN ln -s /lib/node_modules/rpc-websockets/dist/lib/client.js /lib/node_modules/rpc-websockets/dist/lib/client.cjs &&\
    ln -s /lib/node_modules/rpc-websockets/dist/lib/client/websocket.js /lib/node_modules/rpc-websockets/dist/lib/client/websocket.cjs
COPY --from=builder /app/lib/ ./lib/

EXPOSE 9464

CMD ["node", "./lib/index.js"]
