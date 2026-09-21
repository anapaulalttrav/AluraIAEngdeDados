# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Camada Gold — Pontualidade
# MAGIC %md
# MAGIC # Camada Gold — Análise de Pontualidade e Atrasos da Aviação Brasileira
# MAGIC
# MAGIC Pipeline de carga das tabelas Gold a partir das tabelas Silver (VRA, empresas, aerodromos, codigos_operacao), com agregações analíticas por empresa, rota, aeródromo e mês. Inclui queries de insight para identificar quais empresas atrasam mais, em qual época do ano e possíveis causas.

# COMMAND ----------

# DBTITLE 1,1. Schema Gold
# MAGIC %md
# MAGIC ## 1. Criar Schema Gold

# COMMAND ----------

# DBTITLE 1,Criar schema gold
spark.sql("CREATE SCHEMA IF NOT EXISTS voebem.gold")
print("Schema voebem.gold criado/confirmado.")

# COMMAND ----------

# DBTITLE 1,2. atrasos_por_empresa_mes
# MAGIC %md
# MAGIC ## 2. Tabela: gold.atrasos_por_empresa_mes
# MAGIC
# MAGIC Agregação mensal por empresa aerea com metricas de pontualidade, atraso e cancelamento. Esta e a tabela principal para analise de sazonalidade e comparacao entre empresas.

# COMMAND ----------

# DBTITLE 1,Criar atrasos_por_empresa_mes
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.atrasos_por_empresa_mes AS
SELECT
  v.icao_empresa,
  e.razao_social,
  e.sigla_iata,
  YEAR(v.partida_prevista_data)  AS ano,
  MONTH(v.partida_prevista_data) AS mes,
  COUNT(*) AS total_voos,
  SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END) AS voos_realizados,
  SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS voos_cancelados,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_partida_min END), 1)  AS atraso_partida_medio_min,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_chegada_min END),  1)  AS atraso_chegada_medio_min,
  ROUND(100.0 * SUM(CASE WHEN v.atraso_partida_min > 15 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END), 0), 1) AS pct_atraso_15min_partida,
  ROUND(100.0 * SUM(CASE WHEN v.atraso_chegada_min > 15 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END), 0), 1) AS pct_atraso_15min_chegada,
  ROUND(100.0 * SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END)
        / COUNT(*), 1) AS pct_cancelamento,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.minutos_recuperados END), 1) AS recuperacao_media_min
FROM voebem.silver.vra v
LEFT JOIN voebem.silver.empresas e ON v.icao_empresa = e.icao
GROUP BY v.icao_empresa, e.razao_social, e.sigla_iata,
         YEAR(v.partida_prevista_data), MONTH(v.partida_prevista_data)
""")

count1 = spark.table("voebem.gold.atrasos_por_empresa_mes").count()
print(f"gold.atrasos_por_empresa_mes: {count1} linhas")

# COMMAND ----------

# DBTITLE 1,3. pontualidade_por_empresa
# MAGIC %md
# MAGIC ## 3. Tabela: gold.pontualidade_por_empresa
# MAGIC
# MAGIC Resumo consolidado por empresa em todo o periodo (ago/2025 a ago/2026). Ranking definitivo de pontualidade por companhia aerea.

# COMMAND ----------

# DBTITLE 1,Criar pontualidade_por_empresa
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.pontualidade_por_empresa AS
SELECT
  v.icao_empresa,
  e.razao_social,
  e.sigla_iata,
  e.servico,
  COUNT(*) AS total_voos,
  SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END) AS voos_realizados,
  SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS voos_cancelados,
  ROUND(100.0 * SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento_pct,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_partida_min END), 1) AS atraso_medio_partida_min,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_chegada_min  END), 1) AS atraso_medio_chegada_min,
  ROUND(100.0 * SUM(CASE WHEN v.atraso_partida_min > 15 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END), 0), 2) AS pct_atraso_15min,
  ROUND(100.0 * SUM(CASE WHEN v.atraso_partida_min > 60 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END), 0), 2) AS pct_atraso_60min,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.minutos_recuperados END), 1) AS recuperacao_media_min
FROM voebem.silver.vra v
LEFT JOIN voebem.silver.empresas e ON v.icao_empresa = e.icao
GROUP BY v.icao_empresa, e.razao_social, e.sigla_iata, e.servico
HAVING COUNT(*) >= 100
""")

count2 = spark.table("voebem.gold.pontualidade_por_empresa").count()
print(f"gold.pontualidade_por_empresa: {count2} empresas com pelo menos 100 voos")

# COMMAND ----------

# DBTITLE 1,4. atrasos_por_rota_mes
# MAGIC %md
# MAGIC ## 4. Tabela: gold.atrasos_por_rota_mes
# MAGIC
# MAGIC Agregacao por rota (origem-destino) e mes, com nomes dos aerodromos enriquecidos. Para identificar corredores aereos problematicos.

# COMMAND ----------

# DBTITLE 1,Criar atrasos_por_rota_mes
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.atrasos_por_rota_mes AS
SELECT
  v.icao_origem,
  o.nome  AS nome_origem,
  o.uf_nome AS uf_origem,
  v.icao_destino,
  d.nome  AS nome_destino,
  d.uf_nome AS uf_destino,
  YEAR(v.partida_prevista_data)  AS ano,
  MONTH(v.partida_prevista_data) AS mes,
  COUNT(*) AS total_voos,
  SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END) AS voos_realizados,
  SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS voos_cancelados,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_partida_min END), 1) AS atraso_partida_medio_min,
  ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_chegada_min  END), 1) AS atraso_chegada_medio_min,
  ROUND(100.0 * SUM(CASE WHEN v.atraso_partida_min > 15 THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN v.situacao_voo = 'REALIZADO' THEN 1 ELSE 0 END), 0), 1) AS pct_atraso_15min
FROM voebem.silver.vra v
LEFT JOIN voebem.silver.aerodromos o ON v.icao_origem  = o.icao
LEFT JOIN voebem.silver.aerodromos d ON v.icao_destino = d.icao
GROUP BY v.icao_origem, o.nome, o.uf_nome,
         v.icao_destino, d.nome, d.uf_nome,
         YEAR(v.partida_prevista_data), MONTH(v.partida_prevista_data)
HAVING COUNT(*) >= 10
""")

count3 = spark.table("voebem.gold.atrasos_por_rota_mes").count()
print(f"gold.atrasos_por_rota_mes: {count3} linhas (rotas com >= 10 voos/mes)")

# COMMAND ----------

# DBTITLE 1,5. voos_por_aerodromo_mes
# MAGIC %md
# MAGIC ## 5. Tabela: gold.voos_por_aerodromo_mes
# MAGIC
# MAGIC Agregacao por aerodromo e mes, somando voos de origem e destino. Para identificar aeroportos com maior contribuicao nos atrasos.

# COMMAND ----------

# DBTITLE 1,Criar voos_por_aerodromo_mes
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.voos_por_aerodromo_mes AS
WITH origem AS (
  SELECT
    v.icao_origem AS icao, a.nome, a.uf_nome,
    YEAR(v.partida_prevista_data) AS ano, MONTH(v.partida_prevista_data) AS mes,
    COUNT(*) AS voos_origem,
    ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_partida_min END), 1) AS atraso_medio_partida_min,
    SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelamentos_origem
  FROM voebem.silver.vra v
  LEFT JOIN voebem.silver.aerodromos a ON v.icao_origem = a.icao
  GROUP BY v.icao_origem, a.nome, a.uf_nome, YEAR(v.partida_prevista_data), MONTH(v.partida_prevista_data)
),
destino AS (
  SELECT
    v.icao_destino AS icao,
    YEAR(v.partida_prevista_data) AS ano, MONTH(v.partida_prevista_data) AS mes,
    COUNT(*) AS voos_destino,
    ROUND(AVG(CASE WHEN v.situacao_voo = 'REALIZADO' THEN v.atraso_chegada_min END), 1) AS atraso_medio_chegada_min,
    SUM(CASE WHEN v.situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelamentos_destino
  FROM voebem.silver.vra v
  GROUP BY v.icao_destino, YEAR(v.partida_prevista_data), MONTH(v.partida_prevista_data)
)
SELECT
  COALESCE(o.icao, dt.icao) AS icao,
  o.nome, o.uf_nome,
  COALESCE(o.ano, dt.ano) AS ano,
  COALESCE(o.mes, dt.mes) AS mes,
  COALESCE(o.voos_origem, 0) AS voos_origem,
  COALESCE(dt.voos_destino, 0) AS voos_destino,
  COALESCE(o.voos_origem, 0) + COALESCE(dt.voos_destino, 0) AS voos_totais,
  o.atraso_medio_partida_min,
  dt.atraso_medio_chegada_min,
  COALESCE(o.cancelamentos_origem, 0) + COALESCE(dt.cancelamentos_destino, 0) AS cancelamentos_totais
FROM origem o
FULL OUTER JOIN destino dt
  ON o.icao = dt.icao AND o.ano = dt.ano AND o.mes = dt.mes
""")

count4 = spark.table("voebem.gold.voos_por_aerodromo_mes").count()
print(f"gold.voos_por_aerodromo_mes: {count4} linhas")

# COMMAND ----------

# DBTITLE 1,6. Comentarios
# MAGIC %md
# MAGIC ## 6. Adicionar comentarios nas tabelas Gold
# MAGIC
# MAGIC Documentacao das tabelas para governanca e descoberta.

# COMMAND ----------

# DBTITLE 1,Aplicar comentarios
spark.sql("""
COMMENT ON TABLE voebem.gold.atrasos_por_empresa_mes IS
  'Gold - agregacao mensal por empresa aerea com metricas de atraso, pontualidade e cancelamento.
   Periodo: ago/2025 a ago/2026. Limiar de atraso: 15 minutos.'
""")
spark.sql("""
COMMENT ON TABLE voebem.gold.pontualidade_por_empresa IS
  'Gold - ranking consolidado de pontualidade por empresa em todo o periodo.
   Filtra empresas com >= 100 voos. Inclui taxa de cancelamento, atraso medio e recuperacao em voo.'
""")
spark.sql("""
COMMENT ON TABLE voebem.gold.atrasos_por_rota_mes IS
  'Gold - agregacao mensal por rota (origem-destino) com nomes dos aerodromos.
   Filtra rotas com >= 10 voos por mes.'
""")
spark.sql("""
COMMENT ON TABLE voebem.gold.voos_por_aerodromo_mes IS
  'Gold - agregacao mensal por aerodromo, combinando voos de origem e destino.
   Para identificar aeroportos com maior contribuicao nos atrasos.'
""")
print("Comentarios aplicados em todas as tabelas gold.")

# COMMAND ----------

# DBTITLE 1,7. Top empresas que atrasam
# MAGIC %md
# MAGIC ## 7. Análise: Quais empresas mais atrasam?
# MAGIC
# MAGIC Ranking das 15 empresas com maior atraso medio de partida, considerando apenas empresas com >= 1000 voos no periodo.

# COMMAND ----------

# DBTITLE 1,Top 15 empresas com maior atraso
display(spark.sql("""
SELECT
  icao_empresa,
  razao_social,
  sigla_iata,
  total_voos,
  taxa_cancelamento_pct,
  atraso_medio_partida_min,
  atraso_medio_chegada_min,
  pct_atraso_15min,
  pct_atraso_60min,
  recuperacao_media_min
FROM voebem.gold.pontualidade_por_empresa
WHERE total_voos >= 1000
ORDER BY atraso_medio_partida_min DESC
LIMIT 15
"""))

# COMMAND ----------

# DBTITLE 1,8. Sazonalidade
# MAGIC %md
# MAGIC ## 8. Análise: Sazonalidade — em qual parte do ano os atrasos são piores?
# MAGIC
# MAGIC Agregacao de todos os voos por mes, mostrando o atraso medio e percentual de voos atrasados (>15 min) ao longo do ano.

# COMMAND ----------

# DBTITLE 1,Atrasos por mes do ano
display(spark.sql("""
SELECT
  ano, mes,
  SUM(total_voos) AS total_voos,
  SUM(voos_realizados) AS voos_realizados,
  SUM(voos_cancelados) AS voos_cancelados,
  ROUND(AVG(atraso_partida_medio_min), 1) AS atraso_partida_medio_min,
  ROUND(AVG(pct_atraso_15min_partida), 1) AS pct_atraso_15min,
  ROUND(AVG(pct_cancelamento), 2) AS pct_cancelamento
FROM voebem.gold.atrasos_por_empresa_mes
GROUP BY ano, mes
ORDER BY ano, mes
"""))

# COMMAND ----------

# DBTITLE 1,9. Empresa x Mes
# MAGIC %md
# MAGIC ## 9. Análise: Atraso médio por empresa e mês (Top 10 empresas)
# MAGIC
# MAGIC Heat-matrix das 10 empresas com mais voos: atraso medio de partida por mes, para visualizar quais empresas pioram em quais meses.

# COMMAND ----------

# DBTITLE 1,Matrix atraso empresa x mes
display(spark.sql("""
WITH top_empresas AS (
  SELECT icao_empresa, razao_social
  FROM voebem.gold.pontualidade_por_empresa
  ORDER BY total_voos DESC
  LIMIT 10
)
SELECT
  t.razao_social AS empresa,
  m.ano, m.mes,
  m.atraso_partida_medio_min,
  m.pct_atraso_15min_partida,
  m.total_voos
FROM voebem.gold.atrasos_por_empresa_mes m
INNER JOIN top_empresas t ON m.icao_empresa = t.icao_empresa
ORDER BY t.razao_social, m.ano, m.mes
"""))

# COMMAND ----------

# DBTITLE 1,10. Top rotas
# MAGIC %md
# MAGIC ## 10. Análise: Top rotas com maior atraso médio
# MAGIC
# MAGIC Identifica as 15 rotas com maior atraso medio de partida, considerando rotas com pelo menos 100 voos no mes.

# COMMAND ----------

# DBTITLE 1,Top 15 rotas com mais atraso
display(spark.sql("""
SELECT
  nome_origem, uf_origem,
  nome_destino, uf_destino,
  SUM(total_voos) AS total_voos_periodo,
  ROUND(AVG(atraso_partida_medio_min), 1) AS atraso_partida_medio_min,
  ROUND(AVG(atraso_chegada_medio_min), 1) AS atraso_chegada_medio_min,
  ROUND(AVG(pct_atraso_15min), 1) AS pct_atraso_15min
FROM voebem.gold.atrasos_por_rota_mes
GROUP BY nome_origem, uf_origem, nome_destino, uf_destino
HAVING SUM(total_voos) >= 500
ORDER BY atraso_partida_medio_min DESC
LIMIT 15
"""))

# COMMAND ----------

# DBTITLE 1,11. Top aeroportos
# MAGIC %md
# MAGIC ## 11. Análise: Aeroportos com maior atraso médio de partida
# MAGIC
# MAGIC Top 15 aeroportos com maior atraso medio de partida, considerando aeroportos com pelo menos 500 voos de origem no periodo.

# COMMAND ----------

# DBTITLE 1,Top 15 aeroportos com maior atraso
display(spark.sql("""
SELECT
  icao, nome, uf_nome,
  SUM(voos_origem) AS voos_origem_periodo,
  ROUND(AVG(atraso_medio_partida_min), 1) AS atraso_medio_partida_min,
  SUM(cancelamentos_totais) AS cancelamentos_periodo,
  ROUND(100.0 * SUM(cancelamentos_totais) / SUM(voos_totais), 2) AS pct_cancelamento
FROM voebem.gold.voos_por_aerodromo_mes
WHERE nome IS NOT NULL
GROUP BY icao, nome, uf_nome
HAVING SUM(voos_origem) >= 500
ORDER BY atraso_medio_partida_min DESC
LIMIT 15
"""))

# COMMAND ----------

# DBTITLE 1,12. Recuperacao em voo
# MAGIC %md
# MAGIC ## 12. Análise: Recuperação em voo — quais empresas mais recuperam atraso?
# MAGIC
# MAGIC Empresas que, apesar de atrasarem na partida, conseguem recuperar tempo em voo. Recuperacao media = atraso_partida - atraso_chegada (positivo = recuperou).

# COMMAND ----------

# DBTITLE 1,Top recuperacao em voo
display(spark.sql("""
SELECT
  icao_empresa,
  razao_social,
  sigla_iata,
  total_voos,
  atraso_medio_partida_min,
  atraso_medio_chegada_min,
  recuperacao_media_min,
  ROUND(atraso_medio_partida_min - atraso_medio_chegada_min, 1) AS recuperacao_efetiva_min
FROM voebem.gold.pontualidade_por_empresa
WHERE total_voos >= 1000
ORDER BY recuperacao_media_min DESC
LIMIT 15
"""))