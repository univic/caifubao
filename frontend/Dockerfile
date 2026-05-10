# 多阶段构建 - 构建阶段
FROM node:20-alpine AS builder

# 构建时传入 VITE_APP_ENV 以控制前端环境标识（DEV/PROD）
# develop 分支 → development → 显示 DEV 徽章
# main 分支   → production  → 显示 PROD 徽章
ARG VITE_APP_ENV=production

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY package*.json ./

# 安装依赖
RUN npm ci

# 复制源代码
COPY . .

# 构建生产版本（显式传递 VITE_APP_ENV 避免 ENV 指令在某些 buildx 版本中失效）
RUN VITE_APP_ENV=${VITE_APP_ENV} npm run build

# 运行阶段 - 使用 nginx
FROM nginx:alpine

# 复制 nginx 配置
COPY nginx.conf /etc/nginx/nginx.conf

# 从构建阶段复制构建产物
COPY --from=builder /app/dist /usr/share/nginx/html

# 暴露端口
EXPOSE 80

# 启动 nginx
CMD ["nginx", "-g", "daemon off;"]
