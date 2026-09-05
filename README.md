# ETL

Проект ETL (Extract, Transform, Load) для обработки и анализа данных с использованием современного стека технологий.

## Технологический стек

### Основные компоненты
- **Airflow** - оркестрация ETL процессов
- **ClickHouse** - колоночная СУБД для аналитики
- **Apache Spark** - обработка данных
- **Apache Superset** - визуализация данных
- **Jupyter Notebook** - интерактивный анализ данных
- **DBT** - трансформация данных

### Инфраструктура
- **Docker** - контейнеризация всех компонентов
- **MinIO** - S3-совместимое хранилище объектов
- **Redis** - кэширование и очереди
- **PostgreSQL** - метаданные Airflow

## Структура проекта

```
.
├── airflow_dockerfile/    # Конфигурация Airflow
├── clickhouse/           # Конфигурация ClickHouse
├── dags/                 # DAG'и Airflow
├── dbt_click/           # DBT модели для ClickHouse
├── jupyter_dockerfile/  # Конфигурация Jupyter
├── plugins/             # Плагины Airflow
├── scripts/            # Вспомогательные скрипты
├── src/                # Исходный код
├── superset_dockerfile/ # Конфигурация Apache Superset
└── s3_storage/         # Хранилище MinIO
```

## Установка и запуск

1. Клонируйте репозиторий:
```bash
git clone https://github.com/OliskoNikita/ETL.git
cd ETL
```

2. Запустите проект с помощью Docker Compose:
```bash
docker-compose up -d
```

## Конфигурация

### Порты и доступ
- **Airflow**: http://localhost:8080 (admin/admin)
- **ClickHouse**: 
  - HTTP: 8123
  - Native: 9000
- **Superset**: http://localhost:8088
- **Jupyter**: http://localhost:10000
- **MinIO Console**: http://localhost:9001

### Тома данных
- `./data_lake` - основное хранилище данных
- `./s3_storage` - MinIO хранилище
- `./superset_data` - данные Superset

### Логи
- Логи Airflow доступны в директории `./logs`
