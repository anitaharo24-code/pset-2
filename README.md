Proyecto PSet 2
 PSet 2 — NY Taxi Data Pipeline
 Fundamentos de Ciencia de Datos

## Objetivo
Construir una solución end-to-end para ingerir, almacenar, transformar y modelar datos de NY Taxi correspondientes al período 2021–2025.

## Arquitectura
Se levanta mediante Docker Compose y contiene, los siguientes servicios:
● PostgreSQL
● Mage
● pgAdmin
El flujo es el siguiente:
Fuente NY Taxi → Mage pipeline raw → PostgreSQL schema raw → Mage pipeline clean → PostgreSQL schema clean → pgAdmin

## Stack tecnológico
- Docker Compose
- Mage AI (orquestador)
- PostgreSQL 13
- pgAdmin 4

## Estructura del proyecto
pset-2/
├── data-orquestador
├── Notebooks
├── screenshots 
├── .env 
├── docker-compose.yaml 
├── Dockerfile 
├── README.md 
└── requirements 
└── architecture_diagram.png

## Cómo levantar el entorno

### Requisitos previos
- Docker Desktop instalado
- Git instalado

### Pasos

1. Clonar el repositorio:
```bash
git clone https://github.com/anitaharo24-code/pset-2.git
cd pset-2
```

2. Crear el archivo .env basado en .env.example:
```bash
cp .env.example .env
```

3. Completar las variables en .env:
POSTGRES_USER=root
POSTGRES_PASSWORD=root
POSTGRES_DB=warehouse
POSTGRES_HOST=data-warehouse
POSTGRES_PORT=5432
PGADMIN_DEFAULT_EMAIL=tu@email.com
PGADMIN_DEFAULT_PASSWORD=root
MAGE_PROJECT_NAME=orquestador

4. Levantar los servicios:
```bash
docker compose up -d
```

5. Verificar que están corriendo:
```bash
docker compose ps
```

## Acceso a los servicios

| Mage | http://localhost:6789 
| pgAdmin | http://localhost:9000 | valor en .env | valor en .env |

## Cómo ejecutar los pipelines

### Pipeline raw
1. Abrir Mage en http://localhost:6789
2. Ir a raw_pipeline
3. Clic en **Run pipeline → Run now**

### Pipeline clean
1. Ir a clean_pipeline
2. Clic en **Run pipeline → Run now**
3. El sensor verificará que raw_pipeline terminó antes de continuar

## Cómo acceder a pgAdmin
1. Abrir http://localhost:9000
2. Iniciar sesión con las credenciales del .env
3. Ir a Servers → data-warehouse → Databases → warehouse

## Cómo validar resultados

### Schema raw
```sql
SELECT raw_year, raw_month, COUNT(*) as filas
FROM raw.ny_taxi
GROUP BY raw_year, raw_month
ORDER BY raw_year, raw_month;
```

### Schema clean
```sql
SELECT raw_year, COUNT(*) as viajes
FROM clean.fact_trips
GROUP BY raw_year
ORDER BY raw_year;
```

## Modelo dimensional

### Tabla de hechos
**clean.fact_trips** — granularidad: un registro por viaje

### Dimensiones

|           Tabla            |  Descripción         |

|  clean.dim_vendor          |  Proveedores de taxi |
|  clean.dim_payment_type    |  Tipos de pago       |
| clean.dim_pickup_location  |  Zonas de recogida   |
| clean.dim_dropoff_location |  Zonas de destino    |

## Decisiones de diseño

### Dataset
Se utilizó Green Taxi (2021-2025) que tiene un volumen manejable (~50,000 filas/mes). 

### Capa raw
- Dispone transformaciones mínimas nombres snake case
- Se añadieron columnas: raw_year, raw_month, raw_source_url
- Idempotente: no duplica meses ya cargados

### Capa clean
- Muestra valores de inconsistencias 
- Muestra valores de nulos 
- Muestra valores de duplicados
- Elimina registros con distancia <= 0 o > 500 millas
- Elimina registros con tarifa <= 0 o > $1000
- Elimina viajes con 0 pasajeros o más de 6
- Elimina viajes donde dropoff <= pickup
- Elimina viajes de más de 24 horas
- Elimina duplicados con SELECT DISTINCT

### Triggers

raw_taxi_monthly_trigger, mensual, ya que los datos se encuentran en tablas mensauales, es lógico la actualozación mensual.
clean_trigger, también mensual, sin embargo el clean_pipeline tiene un sensor que verifica que raw_pipeline terminó exitosamente antes de ejecutarse.

### Secrets
Las credenciales se manejan mediante secrets de Mage y 
variables de entorno en .env. Ninguna credencial está 
hardcodeada en el código fuente.

