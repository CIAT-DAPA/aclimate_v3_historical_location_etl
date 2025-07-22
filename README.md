# AClimate v3 Historical Location ETL

ETL para procesamiento de datos históricos de ubicaciones de AClimate v3.

## Características

- **Integración con ORM**: Utiliza `aclimate_v3_orm` para operaciones de base de datos
- **Schemas tipados**: Todos los datos se manejan a través de schemas ya formateados
- **Monitoreo con OpenTelemetry**: Integración completa con SigNoz para observabilidad
- **Pipeline CI/CD**: Automated testing, building, and deployment
- **Configuración flexible**: Archivo de configuración de entorno completo
- **Logging estructurado**: Logs detallados para debugging y monitoreo

## Configuración

### Variables de entorno

1. **Copiar el archivo de ejemplo:**

   ```bash
   cp env.example .env
   ```

2. **Configurar variables principales:**
   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`: Conexión a base de datos
   - `OTEL_EXPORTER_OTLP_ENDPOINT`: Endpoint de OpenTelemetry para monitoreo
   - `OTEL_EXPORTER_OTLP_HEADERS`: Token de acceso para SigNoz
   - `LOG_LEVEL`: Nivel de logging (DEBUG, INFO, WARNING, ERROR)

### Dependencias

El proyecto requiere `aclimate_v3_orm` para operaciones de base de datos:

```bash
pip install aclimate_v3_orm
```

## Instalación

### Desde GitHub (recomendado)

```bash
# Instalación básica
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git

# Instalación desde una rama específica
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git@main

# Instalación con dependencias de desarrollo
pip install "git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git[dev]"

# Instalación desde un tag/release específico
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git@v0.1.0
```

### Desde el código fuente

```bash
git clone https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git
cd aclimate_v3_historical_location_etl
pip install -e .
```

### Para desarrollo

```bash
pip install -e ".[dev]"
```

## Uso

```python
from aclimate_v3_historical_location_etl.tools.logging_manager import LoggingManager

# Inicializar el logging manager
log_manager = LoggingManager(
    service_name="my_etl_service",
    log_file="my_etl.log"
)

# Usar el logging
log_manager.info("Proceso iniciado", component="etl_main")
log_manager.warning("Advertencia procesando datos", component="data_processor")
log_manager.error("Error en conexión", component="database", error_code="DB001")
```

### Configuración de SigNoz (opcional)

1. Copia el archivo de ejemplo:

   ```bash
   cp .env.example .env
   ```

2. Configura las variables en `.env`:
   ```env
   ENABLE_SIGNOZ=true
   OTLP_ENDPOINT=your-signoz-endpoint:4317
   LOG_FILE_PATH=logs/etl.log
   ```

## Desarrollo

### 🚀 Configuración inicial

```bash
# Clonar el repositorio
git clone https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git
cd aclimate_v3_historical_location_etl

# Opción 1: Solo instalar dependencias
python dev.py install

# Opción 2: Configurar entorno de desarrollo completo
python dev.py setup
```

### 🛠️ Comandos de desarrollo

```bash
# Instalar solo las dependencias
python dev.py install

# Configurar entorno completo de desarrollo
python dev.py setup

# Ejecutar tests
python dev.py test

# Verificar calidad de código
python dev.py quality

# Formatear código
python dev.py format

# Construir paquete
python dev.py build

# Limpiar archivos generados
python dev.py clean
```

### 📋 Comandos individuales

```bash
# Tests con coverage
pytest --cov=src/ --cov-report=html

# Formatear código
black src/ tests/

# Verificar estilo
flake8 src/ tests/

# Verificar tipos
mypy src/

# Pre-commit hooks
pre-commit run --all-files
```

## Crear distribución

```bash
python -m build
```

## Crear un release

1. Actualiza la versión en `pyproject.toml`
2. Haz commit y push de los cambios
3. Crea un tag:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
4. GitHub Actions automáticamente creará el release

## CI/CD Pipeline con GitHub Actions

El proyecto incluye un pipeline moderno de CI/CD que maneja el flujo completo de desarrollo:

### 🔄 Flujo de Branches

```
develop → PR a stage → stage → main → release
```

### 📋 Workflows disponibles

#### 1. **Pipeline Principal** (`pipeline.yml`)

Se ejecuta **SOLO** en:

- Pull requests hacia `stage`
- Push a `stage` (después de merge del PR)

**Fases del Pipeline:**

- **Testing**: Ejecuta en Python 3.10, 3.11, 3.12

  - Linting con flake8
  - Formateo con black
  - Type checking con mypy
  - Tests con pytest + coverage
  - Upload a Codecov

- **Build**: Construcción del paquete (solo en push a `stage`)

  - Crea distribución con `python -m build`
  - Valida con twine
  - Sube artefactos

- **Auto-merge**: `stage` → `main` (automático después de tests exitosos)

#### 2. **Build and Test** (`build.yml`)

Testing continuo para todas las branches principales

#### 3. **Release** (`release.yml`)

- **Auto-release**: Se ejecuta automáticamente en push a `main`

  - Genera tag automáticamente
  - Actualiza versión en pyproject.toml
  - Crea release con múltiples formatos
  - Sube assets (wheel, tar.gz, zip legacy)

- **Manual release**: Para tags manuales

### 🚀 Cómo usar el pipeline

1. **Desarrollo en develop**:

   ```bash
   git checkout develop
   git add .
   git commit -m "feat: nueva funcionalidad"
   git push origin develop
   ```

   ⚠️ **Nota**: No se ejecuta ningún pipeline en `develop`

2. **Pull Request a stage**:

   ```bash
   # Crear PR desde develop hacia stage
   gh pr create --base stage --head develop --title "Release v1.0.0"
   ```

   ✅ **Se ejecuta**: Testing en múltiples versiones de Python

3. **Merge del PR a stage**:

   ```bash
   # Después de aprobar el PR, se hace merge
   ```

   ✅ **Se ejecuta**: Build + Auto-merge a `main`

4. **Push a main (automático)**:

   ```bash
   # El pipeline automáticamente hace push a main
   ```

   ✅ **Se ejecuta**: Auto-release con nuevo tag

5. **Instalación del release**:
   ```bash
   pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git@v1.0.0
   ```

### 📊 Badges de estado

Agrega estos badges a tu README:

```markdown
![Build Status](https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl/workflows/AClimate%20v3%20Historical%20Location%20ETL%20Pipeline/badge.svg)
![Release](https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl/workflows/Release%20and%20Deploy/badge.svg)
![Coverage](https://codecov.io/gh/CIAT-DAPA/aclimate_v3_historical_location_etl/branch/main/graph/badge.svg)
```

## Licencia

MIT License - ver archivo LICENSE para más detalles.
