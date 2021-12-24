FROM openjdk:14-alpine
COPY ./target/scala-2.13/lb.jar /app/src/app.jar
WORKDIR /app/src
#ENTRYPOINT ["java", "-jar","app.jar"]
ENTRYPOINT ["java", "-Xmx1G","-cp","app.jar","mx.cinvestav.Main"]
