 FROM eclipse-temurin:25-jdk

WORKDIR /app

COPY . .

RUN javac WebChatServer.java

CMD ["java", "WebChatServer"]
