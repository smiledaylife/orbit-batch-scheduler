# 构建调度中心（JRE 11）: docker build -t orbit-admin:1.0.0 --build-arg MODULE=orbit-admin --build-arg RUNTIME_IMAGE=eclipse-temurin:11-jre .
# 构建执行器示例（JRE 8）: docker build -t orbit-executor-sample:1.0.0 --build-arg MODULE=orbit-executor-sample .
#
# 说明：reactor 中 orbit-admin 依赖 Quartz 2.5.x（要求 JDK 11），因此构建阶段统一用 JDK 11；
#      orbit-executor / orbit-core 通过 maven.compiler.release=8 产出 Java 8 字节码，运行阶段可使用 JRE 8 镜像。
ARG MODULE=orbit-admin
ARG BUILD_IMAGE=maven:3.9-eclipse-temurin-11
ARG RUNTIME_IMAGE=eclipse-temurin:8-jre

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
COPY --from=build /src/${MODULE}/target/${MODULE}-1.0.0.jar /app/app.jar
ENV JAVA_OPTS=""
EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
