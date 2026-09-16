# ✈️ VoeBem Analytics — Pipeline de Dados & IA (Imersão Alura)

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-000000?style=for-the-badge&logo=delta-lake&logoColor=white)](https://delta.io/)

> 🚨 **Status do Projeto:** 🏗️ *Em Construção / Processo de Ingestão na Camada Bronze*

---

## 📌 Visão Geral do Projeto

O **VoeBem Analytics** é uma solução de Engenharia e Inteligência de Dados desenvolvida durante a **Imersão de Engenharia de Dados da Alura**. 

O objetivo do projeto é processar, estruturar e analisar volumes massivos de dados históricos de voos (atrasos, cancelamentos, horários, companhias aéreas e aeroportos) para responder a perguntas estratégicas de negócio e apoiar a tomada de decisão sobre a saúde operacional da companhia aérea **VoeBem**.

---

## ❓ Perguntas de Negócio a Serem Respondidas

A arquitetura e os agentes de IA do projeto foram desenhados para responder às seguintes questões:

* **Quais voos mais atrasam?**
* **Início de ano apresenta maior volume de voos atrasados?**
* **Quais são os dias, horários e rotas de maior incidência de atraso?**
* **Quais companhias aéreas e aeroportos concentram os maiores índices de indisponibilidade?**

---

## 🏗️ Arquitetura & Conceitos de Engenharia de Dados

### 1. ⚡ Computação Distribuída & Apache Spark
* **Engine de Processamento:** Uso do **Apache Spark** dentro do **Databricks** para gestão eficiente de recursos, memória e processamento paralelo em larga escala.
* **Consumo & Consultas:** Expressão de transformações e consultas via **PySpark** e **SQL** para otimização de *throughput*.

### 2. 🗄️ Nuvem, Data Lake & Lakehouse
* **Storage de Alta Performance:** Infraestrutura em nuvem (**AWS / Azure**) garantindo alta velocidade de leitura e escrita.
* **Data Lake & Volumes:** Armazenamento de dados brutos com camada de abstração para gerenciamento de arquivos.
* **Arquitetura LakeHouse:** Suporte a **ACID Transactions** e **Versionamento de Dados (Time Travel)**, permitindo updates, rools e modificações pontuais sem necessidade de reprocessar todo o dataset.

---

## 🔄 Status do Pipeline & Camada Bronze

Atualmente, o projeto está na etapa de estruturação da **Camada Bronze (Raw Data/Ingestão)** dentro do banco de dados `voe_bem`.

### ⚙️ Princípios Aplicados na Ingestão:

* **Ingestão Full com Overwrite (`.mode("overwrite")`):** Reescreve os dados brutos da camada Bronze para garantir integridade diante de atualizações de arquivos históricos.
* **Idempotência:** Validação e garantia de que execuções repetidas do pipeline não gerem duplicidade de registros através de filtragens e consistência de ingestão.

---

## 📊 Governança de Dados & Gestão de Metadados

A governança do projeto foi estruturada utilizando o catálogo do **Databricks Unity Catalog**:

* **Catálogo e Esquema:** Documentação ativa das tabelas do banco de dados `voe_bem`.
* **Metadados Contextualizados:** Mapeamento do dicionário de dados (descrição de colunas, comportamento dos tipos de dados e finalidade) para leitura e entendimento autônomo.

---

## 🤖 Consumo via Agentes de IA

Integração de **Agentes de IA** no ambiente do Databricks para consulta inteligente de dados:

* **Validação de Contexto:** Agentes configurados com o contexto organizacional da **VoeBem Analytics**.
* **Consultas em Linguagem Natural:** Capacidade do agente de traduzir perguntas de negócios (ex: *"Quais aeroportos tiveram mais atrasos no 1º trimestre?"*) em consultas otimizadas sobre o pipeline.

---

## 🛠️ Tech Stack & Ferramentas

| Categoria | Tecnologia |
| :--- | :--- |
| **Plataforma de Dados** | Databricks |
| **Processamento Massivo** | Apache Spark (PySpark) |
| **Linguagens** | Python, SQL |
| **Arquitetura de Dados** | Lakehouse / Delta Lake |
| **Cloud Provider** | AWS / Azure |
| **Inteligência Artificial** | Databricks AI Agents |
| **Governança** | Unity Catalog / Metadata Management |

---

## 🚀 Próximos Passos
- [x] Criação do Banco de Dados `voe_bem`
- [x] Carga da Camada **Bronze** com Ingestão Full (Overwrite)
- [ ] Construção da Camada **Silver** (Limpeza, Deduplicação e Padronização)
- [ ] Construção da Camada **Gold** (Agregações e Modelagem Dimensional para BI)
- [ ] Implantação Final do Agente de IA para Relatórios de Saúde Operacional

---
