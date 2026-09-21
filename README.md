# ✈️ VoeBem Analytics — Pipeline de Dados & IA (Imersão Alura)

![Status](https://img.shields.io/badge/Status-Concluído%20%E2%9C%85-brightgreen)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-025E8D?style=flat&logo=sqlite&logoColor=white)

---

## 📌 Visão Geral do Projeto

O **VoeBem Analytics** é uma solução completa de **Engenharia de Dados e Inteligência Artificial** desenvolvida durante a *Imersão de Engenharia de Dados da Alura*. 

O objetivo do projeto é processar, estruturar e analisar volumes massivos de dados históricos de voos (atrasos, cancelamentos, horários, companhias aéreas e aeroportos) sob a arquitetura **Medallion (Bronze, Silver e Gold)**, respondendo a perguntas estratégicas de negócio e apoiando a tomada de decisão sobre a saúde operacional da companhia aérea VoeBem.

---

## 💡 Valor Agregado ao Negócio

O **VoeBem Analytics** transforma dados operacionais brutos em inteligência acionável, entregando valor direto nas seguintes frentes:

* 💰 **Redução de Impacto Financeiro:** Identificação precisa dos gargalos de atraso e indisponibilidade, permitindo renegociação de *slots* e mitigação de multas por descumprimento de horários.
* ⚙️ **Otimização Operacional baseada em Dados:** Mapeamento de rotas e períodos sazonais com maior incidência de cancelamentos, possibilitando dimensionamento preventivo de tripulação e manutenção de aeronaves.
* 🤖 **Democratização do Acesso à Informação:** Com a integração de **Agentes de IA**, gestores e diretores podem realizar consultas complexas em linguagem natural sem a necessidade de escrever SQL ou código Spark.
* 🔒 **Governança e Confiabilidade:** Garantia de integridade e rastreabilidade dos dados (*Lineage*) com o Unity Catalog e o uso de tabelas **Delta Lake (ACID)**.

---

## 🛠️ Tech Stack & Ferramentas

| Categoria | Tecnologia / Ferramentas |
| :--- | :--- |
| **Plataforma de Dados** | ![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white) |
| **Engine de Processamento** | ![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) |
| **Linguagens de Programação** | ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) ![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white) |
| **Arquitetura & Armazenamento** | ![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta&logoColor=white) ![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white) ![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white) |
| **Governança & Metadados** | ![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-000000?style=for-the-badge&logo=databricks&logoColor=white) |
| **Inteligência Artificial** | ![Databricks AI](https://img.shields.io/badge/AI_Agents-FF3621?style=for-the-badge&logo=openai&logoColor=white) |

---

## ❓ Perguntas de Negócio Respondidas

A arquitetura e os agentes de IA do projeto foram desenhados e otimizados para responder a questões estratégicas:

1. **Gargalos Operacionais:** Quais voos e rotas apresentam o maior índice e tempo médio de atraso?
2. **Análise Sazonal:** O início do ano (alta temporada) apresenta maior volume proporcional de voos atrasados em comparação com outros trimestres?
3. **Padrões Temporais:** Quais são os dias da semana e faixas horárias com maior incidência de imprevistos?
4. **Desempenho de Parceiros:** Quais companhias aéreas parceiras e aeroportos concentram os maiores índices de indisponibilidade e cancelamento?

---

## 🏗️ Arquitetura & Conceitos de Engenharia de Dados

### 1. ⚡ Computação Distribuída & Apache Spark
* **Engine de Processamento:** Utilização do **Apache Spark** no ambiente **Databricks** para gestão eficiente de memória, otimização de consultas e processamento paralelo de grandes volumes de dados.
* **Otimização:** Expressão de transformações rigorosas via **PySpark** e **Spark SQL** garantindo alta performance no consumo de CPU/Memória.

### 2. 🗄️ Arquitetura Lakehouse Medallion (Delta Lake)
Suporte total a transações ACID, versionamento de dados (*Time Travel*) e garantia de consistência entre camadas:

* 🥉 **Camada Bronze (Raw Data):** Ingestão dos dados brutos com aplicação dos princípios de **Idempotência** e **Ingestão Full (`.mode("overwrite")`)**, assegurando que releituras de arquivos históricos não causem duplicidade.
* 🥈 **Camada Silver (Clean & Standardized):** Limpeza, desduplicação, tratamento de valores nulos, tipagem estrita de schema e padronização de formatos de data/hora.
* 🥇 **Camada Gold (Business Metrics):** Tabelas agregadas, modelagem dimensional e visões analíticas prontas para consumo por dashboards de BI e modelos de IA.

### 3. 📊 Governança de Dados & Metadados
* **Unity Catalog:** Mapeamento completo do catálogo e esquemas do banco `voe_bem`.
* **Dicionário de Dados:** Documentação contextualizada de colunas, tipos de dados e regras de negócio para permitir autonomia às equipes analíticas.

---

## 🤖 Consumo Inteligente via Agentes de IA

Integração de **Agentes de IA** no ambiente Databricks para apoio à tomada de decisão:

* **Tradução NL2SQL (Natural Language to SQL):** Capacidade do agente de converter perguntas do usuário (ex.: *"Quais foram os 5 aeroportos com mais atrasos em voos no 1º trimestre?"*) em queries SQL otimizadas diretamente na camada **Gold**.
* **Validação de Contexto:** Agentes configurados com o contexto do negócio de aviação civil para responder com métricas de precisão.

---

## 🚀 Status das Etapas do Projeto

- [x] Criação do Banco de Dados `voe_bem`
- [x] Ingestão e Carga da **Camada Bronze** (Raw Data com garantia de Idempotência)
- [x] Construção da **Camada Silver** (Limpeza, Deduplicação e Padronização de Schemas)
- [x] Construção da **Camada Gold** (Agregações Estratégicas e Modelagem Dimensional)
- [x] Documentação e Governança no **Unity Catalog**
- [x] Implantação e Validação do **Agente de IA** para Relatórios de Saúde Operacional
