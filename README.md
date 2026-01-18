# Retail Analytics Lakehouse

Pipeline end-to-end de análise de dados de varejo utilizando Apache Spark, Delta Lake e Arquitetura Medallion (Bronze, Silver, Gold).

## 📋 Visão Geral

Este projeto implementa um data lakehouse completo para análise de dados de varejo, processando informações de produtos e vendas através de múltiplas camadas de transformação. O objetivo é gerar insights acionáveis como regras de cross-sell, identificação de produtos com baixa performance e candidatos para promoções.

## 🏗️ Arquitetura

O projeto segue a **Arquitetura Medallion**, organizada em três camadas:

```
┌─────────────┐
│   Origem    │  CSV Files (Products & Sales)
└──────┬──────┘
       │
┌──────▼──────┐
│   Bronze    │  Dados brutos ingeridos com particionamento
└──────┬──────┘
       │
┌──────▼──────┐
│   Silver    │  Dados limpos, transformados e enriquecidos
└──────┬──────┘
       │
┌──────▼──────┐
│    Gold     │  Agregações analíticas para consumo
└─────────────┘
```

### Camada Bronze
- **Ingestão bruta** dos dados de produtos e vendas
- Dados particionados por `ingestion_date`
- Formato Delta Lake para versionamento e time travel

### Camada Silver
- **Limpeza e transformação** dos dados
- Enriquecimento com colunas derivadas (ano, mês, dia)
- Junção de dados de vendas com produtos
- Particionamento por `year`, `month`, `day`

### Camada Gold
- **Agregações analíticas** prontas para consumo
- Três principais outputs:
  - **Cross-Sell Rules**: Regras de associação de produtos por dia da semana
  - **Low Sales Products**: Produtos com volume de vendas abaixo do threshold
  - **Promotion Candidates**: Produtos candidatos a campanhas promocionais

## 🛠️ Tecnologias

- **Apache Spark 3.5.1**: Processamento distribuído de dados
- **Delta Lake 3.1.0**: Formato de armazenamento ACID para data lakes
- **Python 3.10.12**: Linguagem de desenvolvimento (gerenciado com pyenv)
- **NumPy 1.26.4**: Geração de dados sintéticos

## 📁 Estrutura do Projeto

```
retail-analytics-lakehouse/
├── common/
│   ├── constants.py          # Constantes e configurações globais
│   └── spark_session.py      # Configuração da sessão Spark
├── data/
│   ├── generate_data.py      # Script de geração de dados sintéticos
│   └── new_data.py           # Script para geração de novos dados
├── jobs/
│   ├── bronze/
│   │   ├── products_bronze.py    # Ingestão de produtos
│   │   └── sales_bronze.py       # Ingestão de vendas
│   ├── silver/
│   │   ├── products_silver.py    # Transformação de produtos
│   │   └── sales_silver.py       # Transformação de vendas
│   └── gold/
│       ├── cross_sell_rules.py        # Regras de cross-sell (FP-Growth)
│       ├── lower_sales_products.py    # Produtos com baixa venda
│       └── promotion_candidates.py    # Candidatos a promoção
├── main.py                   # Orquestração do pipeline
├── requirements.txt          # Dependências Python
└── README.md                # Este arquivo
```

## 🚀 Como Executar

### 1. Pré-requisitos

- **Python 3.10.12** (recomendado usar pyenv para gerenciamento de versões)
- **Java 8 ou 11** (requisito do Spark)

### 2. Configuração do Ambiente Python

#### Usando pyenv (recomendado)

```bash
# Instale a versão específica do Python
pyenv install 3.10.12

# Configure a versão local do projeto
pyenv local 3.10.12

# Verifique a versão
python --version  # Deve exibir: Python 3.10.12
```

### 3. Instalação

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd retail-analytics-lakehouse

# Instale as dependências
pip install -r requirements.txt
```

### 4. Gerar Dados Sintéticos

```bash
python data/generate_data.py
```

Este script cria:
- 120 produtos categorizados (Limpeza, Higiene, Alimentos, Bebidas, etc.)
- 5.000 pedidos com dados de vendas do período janeiro-junho/2025
- Combos de produtos repetidos para facilitar análise de associação
- 10% dos produtos com vendas intencionalmente baixas

### 5. Executar o Pipeline Completo

```bash
python main.py
```

O pipeline executa na seguinte ordem:
1. Bronze: Ingestão de produtos e vendas
2. Silver: Limpeza e transformação
3. Gold: Geração de insights analíticos

## 📊 Outputs Analíticos

### 1. Cross-Sell Rules
Identifica padrões de compra conjunta usando algoritmo **FP-Growth**:
- Produtos que costumam ser comprados juntos
- Segmentado por dia da semana para campanhas direcionadas
- Métricas: confidence, lift, support

**Exemplo de uso**: "Clientes que compram café às segundas-feiras têm 60% de chance de comprar pão"

### 2. Low Sales Products
Produtos com volume total de vendas abaixo do threshold (default: 200 unidades):
- Identifica itens com baixa rotação
- Útil para gestão de estoque e descontinuação

### 3. Promotion Candidates
Produtos que combinam:
- Alto volume de vendas (popularidade)
- Baixo faturamento relativo (oportunidade de margem)

Ideal para campanhas promocionais que aumentam receita sem sacrificar volume.

## ⚙️ Configurações

Principais configurações em `common/constants.py`:

```python
# Threshold para identificar produtos com baixa venda
VALUE_TO_DEFINE_LOW_SALES = 200

# Caminhos do Data Lake
DATA_LAKE = "datalake"
BRONZE_PRODUCTS = f"{DATA_LAKE}/bronze/products"
SILVER_PRODUCTS = f"{DATA_LAKE}/silver/products"
GOLD_CROSS_SELL = f"{DATA_LAKE}/gold/cross_sell_rules"
```

## 🔄 Incrementalidade

O pipeline suporta processamento incremental:
- Novos dados podem ser adicionados com `data/new_data.py`
- Particionamento por data permite processamento eficiente
- Delta Lake oferece operações MERGE e time travel

## 📝 Licença

Este projeto está sob a licença especificada no arquivo [LICENSE](LICENSE).

---

**Desenvolvido para fins educacionais e demonstração de arquitetura de dados moderna.**
