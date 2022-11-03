FROM public.ecr.aws/bitnami/node:14 as ts_yarn
ENV NODE_ENV=production
RUN npm install -g yarn
RUN npm install -g typescript
RUN npm install -g ts-node

FROM ts_yarn as keeper_pkgs
WORKDIR /app
COPY package.json package.json
COPY yarn.lock yarn.lock

RUN yarn

FROM keeper_pkgs as keeper_bot

COPY src src
COPY tsconfig.json tsconfig.json
COPY .env .env

RUN yarn build

EXPOSE 9464

ENTRYPOINT [ "yarn", "run" ]