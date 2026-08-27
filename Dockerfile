# 构建调度中心: docker build -t orbit-admin:1.0.0 --build-arg MODULE=orbit-admin .
# 构建执行器示例: docker build -t orbit-executor-sample:1.0.0 --build-arg MODULE=orbit-executor-sample .
FROM maven:3.9-eclipse-temurin-8 AS build
ARG MODULE=orbit-admin
WORKDIR /src
COPY pom.xml .
COPY orbit-core orbit-core
COPY orbit-admin orbit-admin
COPY orbit-executor orbit-executor
COPY orbit-executor-sample orbit-executor-sample
RUN mvn -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:8-jre
ARG MODULE=orbit-admin
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-1.0.0.jar /app/app.jar
ENV JAVA_OPTS=""
EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
