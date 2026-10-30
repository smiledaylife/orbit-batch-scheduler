# 构建调度中心（JRE 21）: docker build -t orbit-admin:1.0.0 --build-arg MODULE=orbit-admin --build-arg RUNTIME_IMAGE=eclipse-temurin:21-jre .
# 构建执行器示例（JRE 21）: docker build -t orbit-executor-sample:1.0.0 --build-arg MODULE=orbit-executor-sample .
#
# 说明：全模块基于 Spring Boot 3.5 + JDK 21，构建与运行阶段统一使用 JDK/JRE 21。
ARG MODULE=orbit-admin
ARG BUILD_IMAGE=maven:3.9-eclipse-temurin-21
ARG RUNTIME_IMAGE=eclipse-temurin:21-jre

FROM ${BUILD_IMAGE} AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY orbit-core orbit-core
COPY orbit-admin orbit-admin
COPY orbit-executor orbit-executor
COPY orbit-executor-sample orbit-executor-sample
RUN mvn -q -pl ${MODULE} -am package -DskipTests

FROM ${RUNTIME_IMAGE}
ARG MODULE
WORKDIR /app
# 用通配符匹配产物名：版本号写死在这里的话，每次改 <version> 都要同步改 Dockerfile，
# 漏改时构建会以 "not found" 失败。spring-boot repackage 生成的 *.jar.original 不会被匹配到。
COPY --from=build /src/${MODULE}/target/${MODULE}-*.jar /app/app.jar

# ---------------------------------------------------------------------------
# 启动命令必须用 exec 形式（JSON 数组）直接调 java，不能包一层 `sh -c`。
#
# 旧写法 ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"] 配合 K8s 的
#   args: ["--spring.config.additional-location=file:/app/config/admin.yml"]
# 按 POSIX 语义 `sh -c string name [args...]`，第一个额外参数会变成 $0 而**不会**拼到命令末尾，
# 结果 ConfigMap 里的整份配置被静默忽略、且没有任何报错 —— 典型表现为「改 ConfigMap 不生效」，
# admin 用一个没人预期的数据源在跑。exec 形式则天然把 args 追加到 java 命令行。
#
# JAVA_OPTS 由 Spring Boot 的 JAVA_TOOL_OPTIONS 语义接管（镜像内空值时不影响）：
# 需要 JVM 参数时请用 JAVA_TOOL_OPTIONS 或 _JAVA_OPTIONS 环境变量，或覆盖 args。
# ---------------------------------------------------------------------------
ENV JAVA_TOOL_OPTIONS=""
EXPOSE 8080 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
