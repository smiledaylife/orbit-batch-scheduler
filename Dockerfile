# ===========================================================================
# Orbit Batch Scheduler :: 多阶段构建镜像（JDK8 运行时）
# 构建上下文 = 工程根目录：docker build -t orbit-scheduler-sample:1.0.0 .
# ===========================================================================

# ---------- 构建阶段 ----------
FROM maven:3.8.6-openjdk-8 AS builder
WORKDIR /build

# 先拷贝 POM 预下载依赖（利用镜像层缓存）
COPY pom.xml ./
COPY scheduler-core/pom.xml scheduler-core/
COPY scheduler-starter/pom.xml scheduler-starter/
COPY scheduler-sample/pom.xml scheduler-sample/
RUN mvn -B -q dependency:go-offline -pl scheduler-sample -am || true

# 拷贝源码并打包
COPY scheduler-core scheduler-core
COPY scheduler-starter scheduler-starter
COPY scheduler-sample scheduler-sample
RUN mvn -B -q -DskipTests package

# ---------- 运行阶段 ----------
FROM eclipse-temurin:8-jre
WORKDIR /app

# 构建产物固定为 scheduler-sample-1.0.0.jar（父 POM version=1.0.0）
COPY --from=builder /build/scheduler-sample/target/scheduler-sample-1.0.0.jar app.jar

ENV TZ=Asia/Shanghai \
    JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
