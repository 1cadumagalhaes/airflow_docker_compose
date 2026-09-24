# Apache Airflow com Docker Compose

Ambiente local de aprendizado com Apache Airflow 3.3.2, PostgreSQL 18,
LocalExecutor e Docker Compose. O repositorio inclui exemplos oficiais do
Airflow, exemplos com Airsql e um passo a passo que comeca com Python comum e
evolui para um DAG.

## Requisitos

- Docker Engine e Docker Compose
- [uv](https://docs.astral.sh/uv/)
- GNU Make

## Comecar

```bash
git clone https://github.com/1cadumagalhaes/airflow_docker_compose.git
cd airflow_docker_compose
cp .env.example .env
```

Revise `.env` antes de iniciar. Em Linux, ajuste `AIRFLOW_UID` para o resultado
de `id -u`. Para gerar um segredo JWT local, use `openssl rand -base64 24` e
substitua `AIRFLOW__API_AUTH__JWT_SECRET`.

Inicialize o metadata database e suba os servicos:

```bash
make init
make up
```

A interface fica em <http://localhost:8080>. A configuracao local padrao usa o
SimpleAuthManager sem login. Nao reutilize as credenciais e os segredos de
desenvolvimento em ambientes compartilhados ou de producao.

Para parar os servicos, use `make down`. `make clean` tambem apaga o volume de
dados do Postgres local.

## Caminho do tutorial de API para Airflow

O DAG `api_etl_tutorial` foi organizado para mostrar a transicao de um script
Python comum para um workflow Airflow:

1. `dags/lib/launch_api.py` contem os modelos Pydantic, a chamada HTTP, a validacao
   da resposta e a normalizacao dos lancamentos. Ele pode ser executado sem
   Airflow e consulta a API publica:

   ```bash
   uv run --env-file .env python dags/lib/launch_api.py
   ```

   A API limita requisicoes anonimas; use o endpoint de desenvolvimento ou
   aguarde a janela de limite antes de repetir chamadas durante o desenvolvimento.

2. `dags/api_etl_tutorial_dag.py` importa `fetch_launches` e adiciona
   agendamento, parametros de execucao no formulario do Airflow, dependencias
   TaskFlow e persistencia.

3. `dags/lib/db_interface.py` concentra o DDL e o UPSERT das linhas recebidas da
   API. O `id` do lancamento e a chave de conflito, tornando repeticoes
   idempotentes. A consulta de resumo usa `@sql.dataframe` do Airsql.

Os modulos em `dags/lib/` sao importados pelos DAGs, mas ficam fora da descoberta
de DAGs via `dags/.airflowignore`.

O formulario do DAG oferece `limit` (padrao 50, entre 1 e 100) e `mode`
(padrao `detailed`, com opcoes `list`, `normal` e `detailed`). Lancamentos sem
detalhes opcionais permanecem com valores SQL `NULL`.

## Exemplos de DAGs

Os DAGs em `dags/examples/` acompanham os exemplos da documentacao do Apache
Airflow, organizados em ordem didatica:

| Arquivo | Assunto |
| --- | --- |
| `01_hello_world_dag.py` | Primeiro DAG |
| `02_etl_taskflow_dag.py` | ETL com TaskFlow |
| `03_params_form_dag.py` | Parametros e formulario de trigger |
| `04_custom_operator_dag.py` | Operador customizado |
| `05_xcom_exchange_dag.py` | Comunicacao com XCom |
| `06_setup_teardown_dag.py` | Setup e teardown |
| `07_branch_with_labels_dag.py` | Branching e labels |
| `08_skip_and_trigger_rules_dag.py` | Skips e trigger rules |
| `09_dynamic_task_mapping_dag.py` | Dynamic task mapping |
| `10_tutorial_basics_dag.py` | Conceitos basicos do Airflow |
| `11_simple_pipeline_dag.py` | Pipeline com Postgres |

Tambem ha `dags/airsql_dag_run_stats.py`, que demonstra uma consulta ao
metadata database usando `@sql.dataframe`.

## Configuracao

Copie `.env.example` para `.env`. Variaveis principais:

| Variavel | Funcao | Padrao |
| --- | --- | --- |
| `POSTGRES_DB` | Banco do Airflow | `airflow` |
| `POSTGRES_USER` | Usuario do Postgres | `airflow` |
| `POSTGRES_PASSWORD` | Senha local do Postgres | `airflow` |
| `POSTGRES_PORT` | Porta do Postgres no host | `5432` |
| `AIRFLOW_WEB_PORT` | Porta do API server no host | `8080` |
| `AIRFLOW_UID` | UID usado pelos containers no Linux | `1000` no exemplo |
| `AIRFLOW_PROJ_DIR` | Diretorio do projeto montado no container | `.` |
| `LOAD_EXAMPLES` | Ativa os DAGs de exemplo embutidos no Airflow | `false` |
| `SIMPLE_AUTH_MANAGER_ALL_ADMINS` | Desabilita login no ambiente local | `true` |

O Compose monta `dags/`, `logs/`, `config/` e `plugins/` no container. Os dados
do PostgreSQL ficam em `data/postgres/` e nao sao versionados.

## Comandos uteis

```bash
make help       # lista os comandos
make init       # constroi a imagem e inicializa o metadata database
make up         # inicia os servicos
make down       # para os servicos
make logs       # acompanha os logs
make psql       # abre psql no container Postgres
make test       # executa os testes
make lint       # executa Ruff
make format     # formata codigo Python
make typecheck  # executa ty
make sql-lint   # executa sqruff
```

Para rodar verificacoes diretamente:

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest -q
```

Os testes unitarios estao em `tests/` e rodam com `make test`; eles nao chamam a
API externa nem exigem um banco de dados. Para depurar uma execucao completa de
um DAG, cada arquivo inclui `if __name__ == '__main__': dag.test()`. Isso e um
teste de integracao: requer o metadata database configurado e, para este DAG,
acesso a API externa e ao Postgres local definidos em `.env`.

## Layout

```text
.
|-- dags/
|   |-- api_etl_tutorial_dag.py
|   |-- airsql_dag_run_stats.py
|   |-- .airflowignore
|   |-- lib/
|   |   |-- db_interface.py
|   |   `-- launch_api.py
|   `-- examples/
|-- tests/
|-- config/
|-- logs/
|-- data/postgres/
|-- compose.yaml
|-- Dockerfile
|-- Makefile
|-- pyproject.toml
`-- uv.lock
```
