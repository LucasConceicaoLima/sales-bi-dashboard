# Databricks notebook source
# MAGIC %md
# MAGIC # Vendas — exploracao e transformacoes com PySpark
# MAGIC
# MAGIC Estudo complementar ao pipeline de vendas em Pandas/PostgreSQL/Power BI.
# MAGIC Etapas: leitura do CSV, perfil de qualidade, remocao de duplicatas,
# MAGIC exclusao de cancelamentos e calculo de indicadores financeiros.
# MAGIC
# MAGIC **Importante:** esta versao e exploratoria; nao substitui o ETL original.

# COMMAND ----------

from pyspark.sql import functions as F

# No Databricks, substitua pelo caminho do Volume que contem vendas.csv:
# CAMINHO_CSV = "/Volumes/<catalogo>/<schema>/<volume>/vendas.csv"
# Em uma execucao Spark local, use o caminho relativo ao repositorio:
CAMINHO_CSV = "data/raw/vendas.csv"

df_vendas_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(CAMINHO_CSV)
)

df_vendas_raw.show(10, truncate=False)
df_vendas_raw.printSchema()
print(f"Registros brutos: {df_vendas_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Perfil dos dados
# MAGIC
# MAGIC Verificacao de volume, IDs repetidos, duplicatas exatas, valores nulos
# MAGIC em campos obrigatorios e distribuicao por status.

# COMMAND ----------

total_bruto = df_vendas_raw.count()
ids_distintos = df_vendas_raw.select("sale_id").distinct().count()
linhas_distintas = df_vendas_raw.distinct().count()

duplicatas_por_id = total_bruto - ids_distintos
duplicatas_exatas = total_bruto - linhas_distintas

df_ids_repetidos = (
    df_vendas_raw.groupBy("sale_id")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Registros brutos: {total_bruto}")
print(f"IDs distintos: {ids_distintos}")
print(f"Duplicatas exatas: {duplicatas_exatas}")
print(f"Quantidade de IDs repetidos: {df_ids_repetidos.count()}")

df_ids_repetidos.orderBy(F.desc("count"), F.asc("sale_id")).show(10)

# COMMAND ----------

colunas_obrigatorias = [
    "sale_id", "sale_date", "customer_id", "product_id", "seller_id"
]

contagem_nulos = [
    F.sum(F.when(F.col(coluna).isNull(), 1).otherwise(0)).alias(coluna)
    for coluna in colunas_obrigatorias
]

df_vendas_raw.agg(*contagem_nulos).show()
df_vendas_raw.groupBy("order_status").count().orderBy("order_status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpeza
# MAGIC
# MAGIC Os 180 registros excedentes tinham conteudo integralmente duplicado.
# MAGIC Por isso, neste dataset, e seguro deduplicar usando `sale_id`.
# MAGIC Se surgirem IDs repetidos com conteudos diferentes, o codigo interrompe
# MAGIC a limpeza para que a divergencia seja analisada.

# COMMAND ----------

if duplicatas_por_id != duplicatas_exatas:
    raise ValueError(
        "Existem IDs repetidos com dados divergentes. "
        "Investigue antes de aplicar dropDuplicates por sale_id."
    )

df_sem_duplicatas = df_vendas_raw.dropDuplicates(["sale_id"])

df_limpo = df_sem_duplicatas.filter(
    F.lower(F.trim(F.col("order_status"))) != "cancelado"
)

print(f"Registros apos remover duplicatas: {df_sem_duplicatas.count()}")
print(f"Registros apos remover cancelamentos: {df_limpo.count()}")
df_limpo.groupBy("order_status").count().orderBy("order_status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regras financeiras
# MAGIC
# MAGIC - Vendas concluidas geram movimento positivo.
# MAGIC - Devolucoes geram movimento negativo.
# MAGIC - A partir disso, calculamos quantidade liquida, receita bruta,
# MAGIC   descontos, receita liquida, custo total, lucro bruto e margem.
# MAGIC
# MAGIC **Nota:** esta e a etapa mais recente do estudo. A conciliacao dos
# MAGIC indicadores calculados em PySpark com o ETL Pandas ainda esta pendente.
# MAGIC Usamos `double` inferido do CSV; para uso financeiro rigoroso,
# MAGIC sera necessario avaliar `DecimalType` e as regras de arredondamento.

# COMMAND ----------

df_financeiro = (
    df_limpo
    .withColumn(
        "movement_sign",
        F.when(
            F.lower(F.trim(F.col("order_status"))) == "devolvido", -1
        ).otherwise(1)
    )
    .withColumn(
        "net_quantity",
        F.col("quantity") * F.col("movement_sign")
    )
    .withColumn(
        "gross_revenue",
        F.round(
            F.col("unit_price") * F.col("quantity") * F.col("movement_sign"), 2
        )
    )
    .withColumn(
        "discount_amount",
        F.round(
            F.col("unit_price")
            * F.col("quantity")
            * F.col("discount_pct")
            * F.col("movement_sign"),
            2,
        )
    )
    .withColumn(
        "net_revenue",
        F.round(F.col("gross_revenue") - F.col("discount_amount"), 2)
    )
    .withColumn(
        "total_cost",
        F.round(
            F.col("unit_cost") * F.col("quantity") * F.col("movement_sign"), 2
        )
    )
    .withColumn(
        "gross_profit",
        F.round(F.col("net_revenue") - F.col("total_cost"), 2)
    )
    .withColumn(
        "margin_pct",
        F.when(
            F.col("net_revenue") != 0,
            F.round(F.col("gross_profit") / F.col("net_revenue"), 4),
        ).otherwise(0.0)
    )
)

df_financeiro.select(
    "sale_id",
    "order_status",
    "quantity",
    "net_quantity",
    "gross_revenue",
    "discount_amount",
    "net_revenue",
    "total_cost",
    "gross_profit",
    "margin_pct",
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Proximas etapas (nao concluidas)
# MAGIC
# MAGIC 1. Validar os totais financeiros e comparar com o ETL Pandas.
# MAGIC 2. Agregar indicadores por status, regiao e periodo.
# MAGIC 3. Construir dimensoes e fato em PySpark e validar as chaves.
# MAGIC 4. Estudar Delta Lake e arquitetura Bronze / Silver / Gold.
