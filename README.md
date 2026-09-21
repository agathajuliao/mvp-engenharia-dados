# mvp-engenharia-dados
Plataforma Omnichannel de Valuation e Inteligência de A&amp;R Musical
# MVP de Engenharia de Dados: Plataforma Omnichannel de Valuation e A&R Musical

## 1. Visão Geral do Projeto e Problema de Negócio

No mercado fonográfico e editorial moderno, a pulverização do consumo de música entre canais digitais (Spotify, Deezer) e rádio (*Airplay*) gera um enorme desafio de **retenção de receita** (*royalty leakage*). Músicas de alto desempenho no *Top Charts* frequentemente dependem de **coautoria múltipla ($1/N$)**, em que compositores independentes sem contrato editorial ativo deixam de arrecadar centenas de milhares de reais por conflitos ou ausência de cadastro estandardizado de metadados.

Este projeto consiste em um **Produto de Dados (Data Product)** desenvolvido no **Databricks Serverless** com **Unity Catalog**, projetado para:
1. **Harmonizar e Unificar Metadados Omnichannel:** Processar dados brutos do *Crowley Charts* (Digital e Rádio), corrigindo inconsistências cadastrais de grandes gravadoras/editoras (*Majors* como Warner, Sony e Universal) através de técnicas de **Master Data Management (MDM)**.
2. **Executar o Rateio Fracionado de Direitos Autorais ($1/N$):** Desmembrar obras em coautoria múltipla via PySpark, aplicando a divisão proporcional exata sobre a arrecadação.
3. **Mapear Valuation Estimado e Lead Scoring de A&R:** Calcular o faturamento retido por obra/autor e rodar um algoritmo de pontuação (*Lead Score*) para orientar a equipe comercial de A&R (*Artist & Repertoire*) na captação dos compositores independentes mais lucrativos do mercado.

---

## 2. Arquitetura e Governança de Dados

A solução foi estruturada sob a **Arquitetura Medallion** (Camadas Bronze, Silver e Gold) e governada via **Unity Catalog Delta Lake**, garantindo rastreabilidade, histórico de auditoria e alta performance de consulta.

[ Databricks Volumes ] ──> [ Bronze: Ingestão Bruta ] ──> [ Silver: MDM & Split 1/N ] ──> [ Gold: Valuation & Lead Score ] ──> [ Dashboard Executivo ]

### Evidência 1: Carga dos Dados de Origem (Databricks Volumes)
Os dados do relatório oficial da Crowley foram carregados no ambiente de nuvem gerenciado do Databricks.

<img width="3153" height="1647" alt="MVP - PRINT 1" src="https://github.com/user-attachments/assets/5bcff0ba-a7b8-4f22-afdd-0294560cc254" />


### Evidência 2: Estrutura de Tabelas Delta no Unity Catalog
Persistência e governança das tabelas em arquitetura Medallion sob o esquema `workspace.default`.

<img width="1029" height="834" alt="MVP - PRINT 2" src="https://github.com/user-attachments/assets/c167fefc-3484-4d3d-af7a-abf8290c9d4f" />


---

## 3. Engenharia de Dados & Pipeline Medallion

### Camada Bronze (Ingestão)
Carga do arquivo fonte e harmonização inicial dos canais digitais e de rádio, unificando os esquemas e aplicando *bypass* do cabeçalho proprietário da Crowley.

### Camada Silver (Data Quality, MDM e Rateio $1/N$)
* **Master Data Management (MDM):** Padronização de strings (`UPPER`, `TRIM`) e unificação de variações operacionais das editoras (*Warner Chappell*, *Sony Music Publishing*, *Universal Music*).
* **Desmembramento Fracionado ($1/N$):** Divisão por expressão regular da coluna de autores, aplicação de `F.explode()` e cálculo da quantidade de coautores por obra para divisão exata de receita.

### Camada Gold (Valuation & Algoritmo de Lead Score)
* **Motor Financeiro de Valuation:** Aplicação de alíquotas diferenciadas por plataforma (Rádio ECAD vs. Streaming Digital) e divisão pelo fator de coautoria:
 
$$\text{Valuation Obra (BRL)} = \frac{\text{Execuções} \times \text{Taxa Royalty}}{\text{Qtd Coautores}}$$

* **Algoritmo de A&R Lead Score:** Pontuação algorítmica de atratividade comercial (0 a 100) combinando escala logarítmica de faturamento acumulado ($70\%$) e volume de obras no Top Charts ($30\%$):

$$\text{Lead Score} = \left( \frac{\ln(1 + \text{Valuation})}{\ln(1 + \text{Valuation}_{\max})} \times 70 \right) + \left( \min\left(\frac{\text{Qtd Hits}}{5}, 1\right) \times 30 \right)$$

### Evidência 3: Pipeline Executado sem Erros
Confirmação da execução ponta a ponta do pipeline PySpark e gravação nas tabelas Delta Gold.
O pipeline foi executado com sucesso no Databricks via PySpark, seguindo rigorosamente a arquitetura Medalhão. Abaixo estão as evidências de código e execução para cada etapa do processo:

### 4.1. Camada Bronze: Ingestão Multi-Fonte (Omnichannel)
Ingestão do arquivo bruto da Crowley, criação das derivações de canais (Spotify, Deezer e Rádio) e consolidação da tabela inicial.

<img width="2046" height="1044" alt="Camada_Bronze" src="https://github.com/user-attachments/assets/20af529f-e519-4c0c-91f7-813b484af63b" />

### 4.2. Camada Silver: Qualidade de Dados, MDM e Rateio (1/N)
Limpeza de strings, unificação de grandes gravadoras/editoras e desmembramento fracionado dos coautores via `explode()`.

<img width="2184" height="1161" alt="Camada_Silver" src="https://github.com/user-attachments/assets/448c17f8-55b9-45f5-bc87-1933212f76c2" />

### 4.3. Camada Gold: Valuation Financeiro e Regras de Negócio
Aplicação das taxas diferenciadas de royalty por canal e cálculo do faturamento estimado por coautor ($1/N$).

<img width="2046" height="936" alt="Camada_Gold" src="https://github.com/user-attachments/assets/85c72dd9-9777-4ae0-b038-c24cf3b0d7b2" />

### 4.4. Camada Gold: Algoritmo de Lead Score e Persistência Delta
Agregação das métricas comerciais, cálculo logarítmico do Lead Score e confirmação de gravação no Unity Catalog.

<img width="2682" height="1302" alt="Tabelas_Gold" src="https://github.com/user-attachments/assets/81671974-58a9-4897-8405-7f92bb2321b2" />

---

## 4. Dicionário de Dados Integrado

| Coluna | Tipo | Regra de Negócio / Descrição |
| :--- | :--- | :--- |
| `Rank` | `INT` | Posição oficial no ranking de execuções da Crowley |
| `Musica_Clean` | `STRING` | Título padronizado da obra (`UPPER` + `TRIM`) |
| `Artista_Clean` | `STRING` | Intérprete(s) principal(is) do fonograma |
| `Compositor_Nome` | `STRING` | Autor individual desmembrado via operação `explode()` |
| `Qtd_Coautores` | `INT` | Total de coautores identificados na obra (fator $1/N$) |
| `Plataforma` | `STRING` | Canal de origem do consumo (*Spotify*, *Deezer*, *Radio*) |
| `Execucoes` | `BIGINT` | Audiência/Streams auditados no período |
| `Valuation_Obra_RS` | `DOUBLE` | Faturamento estimado de royalties ($R\$$) referente à fração do autor |
| `Lead_Score` | `DOUBLE` | Pontuação algorítmica comercial para prospecção de A&R (0 a 100) |
| `Status_Editora` | `STRING` | Classificação do autor (*SEM EDITORA*, *AUTOADMINISTRADO*, *EDITORA ESTABELECIDA*) |

---

## 5. Dashboard Executivo & Resposta às Perguntas de Negócio

O painel final foi codificado programaticamente em Python (`matplotlib`/`seaborn`) com tema escuro de alto contraste executivo, respondendo diretamente às hipóteses estratégicas de negócio.

### Evidência 4: Dashboard do Produto de Dados

<img width="1645" height="938" alt="dashboard mvp engenharia de dados" src="https://github.com/user-attachments/assets/66e11317-35e2-4d13-a802-c4adf8245992" />


### Insights Principais e Validação das Hipóteses:

1. **Oportunidades de Maior Valor (Oceano Azul de A&R):**
   * **Confirmada.** O pipeline identificou compositores independentes no *Top Charts* gerando faturamento expressivo sem gestão editorial. Nomes como **Marco Esteves** ($R\$$ 384.801 retidos) e **Elvis Elan** ($R\$$ 317.156 retidos) lideram a matriz de prioridade de captação.
2. **Dependência de Coautoria Múltipla ($1/N$):**
   * **Confirmada.** **68,3% das obras** do ranking dependem de parcerias com 3 ou mais autores. Sem a divisão fracionada automatizada, o cálculo de arrecadação geraria distorções severas de faturamento.
3. **Concentração de Mercado (Market Share & HHI):**
   * A editora **Warner Chappell** detém a liderança isolada com **21,9%** do faturamento total do mercado, seguida por Universal Music (11,1%) e Sony Music Publishing (9,1%). A fatia de autores sem editora representa **5,2%** do faturamento global auditado.

---

 Tecnologias Utilizadas
* **Linguagem:** Python 3.x / PySpark
* **Plataforma:** Databricks Serverless
* **Governança:** Unity Catalog & Delta Lake
* **Visualização:** Matplotlib & Seaborn
* **Arquitetura:** Medallion Architecture (Bronze, Silver, Gold)

---

---

## 6. Considerações Finais e Pontos de Melhoria

### Considerações Finais
O desenvolvimento deste **MVP** comprovou que é possível resolver um problema histórico do mercado da música, o vazamento de receita e a desorganização de metadados, combinando engenharia de dados moderna e modelagem financeira. A arquitetura Medallion no Databricks não apenas organizou os dados brutos da Crowley, mas automatizou o cálculo proporcional de coautoria ($1/N$) e entregou uma matriz clara para a tomada de decisão comercial da equipe de A&R.

### Pontos de Melhoria para Evolução do Projeto:
1. **Orquestração Automática de Jobs:** Em um ambiente de produção comercial, migrar da execução manual para um fluxo agendado e monitorado via **Databricks Workflows** ou **Azure Data Factory**.
2. **Ingestão Contínua (Streaming):** Evoluir a Camada Bronze para processar cargas em tempo real (*Structured Streaming*), consumindo APIs diretas do Spotify Web API e YouTube Content ID.
3. **Modelagem Preditiva de Sucessos:** Expandir o algoritmo de *Lead Score* na Camada Gold com modelos de Machine Learning (como *XGBoost*) para prever o potencial de estouro (*hit potential*) de novos lançamentos antes da consolidação do mercado.

---

## 7. Autoavaliação

Desenvolver este MVP foi uma experiência de enorme aprendizado e superação. No início da sprint, encarar o volume de dados e construir a arquitetura do zero no Databricks parecia um desafio intimidador. No entanto, decidir encarar um problema real de negócio me motivou a ir muito além do básico da teoria.

Destaco como principais pontos da minha trajetória neste trabalho:

* **Domínio Prático de PySpark e Governança:** Deixei para trás a insegurança com código para criar rotinas complexas de limpeza de strings, agrupamentos e desmembramento de arrays (`explode()`), além de estruturar com rigor as tabelas no **Unity Catalog**.
* **Foco em Valor de Negócio:** Entendi que a engenharia de dados só cumpre seu papel quando gera impacto. Projetar as taxas de royalty e desenvolver o algoritmo de *Lead Score* me provou como transformar dados confusos em decisões estratégicas de captação de talentos.
* **Autonomia e Confiança Técnica:** Finalizar um projeto que vai da ingestão bruta até um dashboard me deu a certeza de que estou no caminho certo para iniciar no mercado e entregar soluções de dados complexas na vida real.
