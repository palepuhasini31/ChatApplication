FROM eclipse-temurin:25-jdk

WORKDIR /app

COPY . .

RUN javac WebChatServerPublic.java

CMD ["java", "WebChatServerPublic"]
