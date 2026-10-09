FROM node:22-bookworm-slim
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install --no-audit --no-fund \
      @temporalio/client @temporalio/worker @temporalio/workflow @temporalio/activity \
      typescript @types/node
COPY src ./src
COPY fibotime.js ./
RUN npx tsc && npm ls --depth=0 --json > /app/deps.json
