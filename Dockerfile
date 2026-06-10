# Fase 1: Construcción (Build)
FROM hseeberger/scala-sbt:17.0.2_1.6.2_2.12.15 AS builder
WORKDIR /app
COPY . .
# Esto crea el JAR dentro del contenedor sin importar cómo se llame
RUN sbt assembly

# Fase 2: Ejecución (Runtime)
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
# Aquí copiamos el JAR desde la fase 'builder'.
# Usamos un nombre fijo, por ejemplo 'app.jar', independientemente del nombre original
COPY --from=builder /app/target/scala-2.12/*.jar app.jar

# Creamos directorios necesarios
RUN mkdir -p /app/data /app/checkpoints

# Definimos variables de entorno.
# Importante: Asegúrate de que tu 'application.conf' use estas variables
# o que tus procesos las carguen correctamente.
ENV SILVER_PATH="/app/data/silver"
ENV GOLD_PATH="/app/data/gold"
ENV GOLD_CHECKPOINT_DIR="/app/checkpoints/gold"

# ENTRYPOINT: Lanzamos con la opción de especificar la clase principal
# Esto permite que el mismo contenedor ejecute bronze, silver o gold según lo que pases
ENTRYPOINT ["java", "-cp", "app.jar", "com.sensores.main.Main"]