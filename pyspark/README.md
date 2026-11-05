# Estudo de PySpark com Databricks

Este diretório contém uma **implementação exploratória em PySpark** das primeiras etapas do pipeline de vendas do projeto [Sales BI Dashboard](../README.md). O pipeline original em **Python/Pandas + PostgreSQL + Power BI** permanece independente.

## Objetivo

Praticar operações de processamento de dados com Spark DataFrames sobre o mesmo arquivo `data/raw/vendas.csv`, aproximando o estudo das rotinas encontradas em projetos de BI e engenharia de dados.

## O que foi trabalhado

1. Leitura de CSV e inferência de schema com Spark.
2. Exploração de dados com `show`, `printSchema`, `count`, `filter`, `groupBy` e agregações.
3. Verificações de nulos, IDs repetidos e duplicatas exatas.
4. Limpeza usando `dropDuplicates` e filtro de pedidos cancelados.
5. Transformações financeiras usando `withColumn`, `when`, `otherwise` e operações entre colunas.
6. Em exercícios anteriores ao notebook, também foram praticados `join`, `left_anti` e inspeção do plano de execução (`explain`).

## Resultados observados no dataset

| Etapa | Registros |
| --- | ---: |
| Arquivo bruto | 32.180 |
| IDs de venda distintos | 32.000 |
| Linhas duplicadas exatas (excedentes) | 180 |
| Após remover duplicatas | 32.000 |
| Após excluir cancelamentos | 31.217 |

Depois da limpeza:

| Status | Registros |
| --- | ---: |
| Concluído | 30.565 |
| Devolvido | 652 |
| **Total** | **31.217** |

As verificações feitas durante o estudo também identificaram **zero valores nulos** em `sale_id`, `sale_date`, `customer_id`, `product_id` e `seller_id`. As 180 linhas excedentes eram duplicatas exatas, portanto a deduplicação por `sale_id` foi consistente neste conjunto de dados.

## Como executar

Arquivo: [`01_exploracao_vendas.py`](01_exploracao_vendas.py).

É um **notebook Databricks em formato source**, versionado como `.py`. O arquivo inclui separadores `# COMMAND ----------`, que são interpretados como células ao importar no Databricks.

1. Importe o notebook para o workspace Databricks.
2. Envie `data/raw/vendas.csv` para um Unity Catalog Volume.
3. Na primeira célula de código, ajuste `CAMINHO_CSV` para `/Volumes/<catalogo>/<schema>/<volume>/vendas.csv`.
4. Execute as células de cima para baixo. Em uma nova sessão, execute novamente a célula de leitura para recriar `df_vendas_raw`.

O notebook também pode servir como base para uma execução Spark local, desde que exista uma sessão `spark` e o caminho `data/raw/vendas.csv` esteja acessível ao Spark.

## Estado atual e próximos passos

- **Validado na execução do estudo:** leitura, schema, totalizadores, verificação de nulos e duplicatas, exclusão de cancelados e distribuição por status.
- **Implementado para estudo, ainda sem conciliação final:** cálculos financeiros (`net_revenue`, `gross_profit`, `margin_pct` e correlatos).
- **Pendente:** comparar o resultado financeiro com o pipeline Pandas, criar modelagem dimensional em Spark, estudar persistência Delta Lake e arquitetura medalhão (Bronze/Silver/Gold).

> Este notebook é um estudo complementar. Não representa um pipeline produtivo, processamento distribuído em escala ou implantação de arquitetura medalhão concluída.
