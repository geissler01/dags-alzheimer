data-dags/
│
├── dags/                    <-- Aquí viven tus pipelines
│   ├── etl_ventas.py
│   ├── reporte_diario.py
│   └── utils/               (Funciones comunes en Python)
│
├── plugins/                 <-- Plugins personalizados para Airflow
│   └── hooks_externos.py
│
├── requirements.txt         (Librerías Python extra que puedan necesitar tus DAGs)
│
└── .github/                 <-- (Opcional futuro) Para CI/CD
    └── workflows/
        └── deploy.yml       (Para que GitHub avise a n8n cuando hagas un push)