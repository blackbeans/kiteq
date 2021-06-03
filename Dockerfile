# 注：我们的源代码和工作目录都是 /go/src/app/
# 二进制可执行文件统一为 /go/src/app/bootstrap
# 配置文件统一在 /go/src/app/conf


# build 第一阶段：生成可执行二进制
FROM golang:1.16.4 AS builder
LABEL maintainer="blackbeans.zc@gmail.com"

# 处理 ssh key
ARG SSH_PRIVATE_KEY=""
RUN mkdir /root/.ssh
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts


# 设置goproxy
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://mirrors.aliyun.com/goproxy/,direct

# 设置 go 编译参数
# 设置为1后我们的编译产物就是完全不依赖操作系统的 static binary了, 否则scratch等小镜像没法运行
RUN go env -w CGO_ENABLED=0

COPY . /go/src/app/
WORKDIR /go/src/app/

# go构建可执行文件，使用之前的缓存
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -v -o /go/src/app/bootstrap  /go/src/app/kiteq.go

RUN rm /root/.ssh/id_rsa

# build 第二阶段：将二进制包和配置文件放入基础镜像中
FROM scratch

COPY --from=builder /go/src/app/bootstrap /go/src/app/bootstrap
COPY --from=builder /go/src/app/conf  /go/src/app/conf


WORKDIR /go/src/app/

# 最终运行docker的命令
ENTRYPOINT ["./bootstrap"]

