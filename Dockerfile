# 构建调度中心（JRE 21）: docker build -t orbit-admin:1.0.0 --build-arg MODULE=orbit-admin --build-arg RUNTIME_IMAGE=eclipse-temurin:21-jre .
# 构建执行器示例（JRE 21）: docker build -t orbit-executor-sample:1.0.0 --build-arg MODULE=orbit-executor-sample .
#
# 说明：全模块基于 Spring Boot 3.5 + JDK 21，构建与运行阶段统一使用 JDK/JRE 21；
#      仅 orbit-core 协议模型保持 Java 8 字节码（可被任意 JRE 解析，但本镜像体系统一 21）。
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
ENV JAVA_OPTS=""
EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
