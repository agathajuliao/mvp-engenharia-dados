# MVP de Engenharia de Dados: Plataforma Omnichannel de Valuation e Inteligência de A&R Musical

## Sumário
1. [Contexto de Negócio e Perguntas do Projeto](#1-contexto)
2. [Arquitetura e Carga dos Dados](#2-arquitetura)
3. [Pipeline de Dados e Qualidade de Dados (Medallion)](#3-pipeline)
4. [Modelagem e Catálogo de Dados](#4-catalogo)
5. [Análise de Dados e Resposta às Perguntas de Negócio](#5-analise)
6. [Considerações Finais e Pontos de Melhoria](#6-consideracoes)
7. [Autoavaliação](#7-autoavaliacao)

---

## <a id="1-contexto"></a>1. Contexto de Negócio e Perguntas do Projeto

No mercado da música, o consumo de faixas ocorre de forma dividida entre canais digitais (como Spotify e Deezer) e execuções nas rádios (*Airplay*). Essa divisão dificulta o acompanhamento de quem deve receber os direitos autorais por cada reprodução, gerando perdas financeiras (*royalty leakage*).

O problema principal acontece na **coautoria múltipla ($1/N$)**. As músicas mais tocadas nos rankings raramente são feitas por uma única pessoa; em geral, são escritas por grupos de três ou mais compositores. Quando um desses autores é independente (não tem contrato com grandes editoras) ou possui um cadastro com dados incompletos, a fatia de dinheiro que cabe a ele acaba retida em associações ou se perde pela falta de padronização nos registros.

Este projeto desenvolve um **Produto de Dados** no **Databricks Serverless** com **Unity Catalog** para resolver esse problema em três frentes:

1. **Padronização de Cadastro (Master Data Management - MDM):** Limpar e organizar os dados brutos da *Crowley Charts*, unificando nomes de grandes editoras (como Warner, Sony e Universal) para evitar duplicidades.
2. **Rateio de Coautoria ($1/N$):** Usar o PySpark para separar os nomes de todos os compositores de cada música e calcular a divisão exata e proporcional do dinheiro para cada um.
3. **Valuation e Priorização de Captação (Lead Score):** Calcular quanto dinheiro cada autor independente tem a receber e gerar uma nota de atratividade (0 a 100) para ajudar a equipe de A&R (*Artist & Repertoire*) a encontrar os compositores mais promissores para contratar.

> **Licença e Origem dos Dados:** Os dados brutos foram extraídos dos relatórios públicos de auditoria de mercado disponibilizados pela [Crowley Charts](https://charts.crowley.com.br/). A utilização deste conjunto de dados possui finalidade estritamente acadêmica e de pesquisa em Engenharia de Dados, sem fins comerciais.

---

## <a id="2-arquitetura"></a>2. Arquitetura e Carga dos Dados

O projeto foi organizado no padrão de **Arquitetura Medallion** (camadas Bronze, Silver e Gold) e estruturado no **Unity Catalog** usando tabelas **Delta Lake** no esquema `workspace.default`.

```
[ Databricks Volumes ] ──> [ Bronze: Ingestão Bruta ] ──> [ Silver: MDM & Split 1/N ] ──> [ Gold: Valuation & Lead Score ] ──> [ Dashboard Executivo ]
```
### Evidência 1: Carga dos Dados de Origem (Databricks Volumes)
Os arquivos em formato CSV com os dados da Crowley foram carregados no diretório gerenciado do Databricks no caminho `/Volumes/workspace/default/meus_arquivos`.

<img width="3153" height="1647" alt="MVP - PRINT 1" src="https://github.com/user-attachments/assets/5bcff0ba-a7b8-4f22-afdd-0294560cc254" />

### Evidência 2: Estrutura das Tabelas no Unity Catalog
Criação e organização das tabelas nas três camadas Medallion dentro do catálogo do Databricks.

<img width="1029" height="834" alt="MVP - PRINT 2" src="https://github.com/user-attachments/assets/c167fefc-3484-4d3d-af7a-abf8290c9d4f" />

---

## <a id="3-pipeline"></a>3. Pipeline de Dados e Qualidade de Dados (Medallion)

O processamento foi feito com scripts em PySpark para transformar os dados brutos até a entrega final.

### Funcionamento das Camadas:
* **Camada Bronze (Ingestão):** Lê os arquivos brutos, remove linhas de cabeçalho desnecessárias da Crowley e unifica as colunas dos canais de streaming e rádio na tabela `bronze_omnichannel`.
* **Camada Silver (Limpeza, MDM e Divisão $1/N$):** Aplica funções de limpeza de texto (`UPPER`, `TRIM`) para padronizar os nomes de editoras (como unificar variações de *Warner Chappell*, *Sony Music* e *Universal Music*). Em seguida, usa a função `F.explode()` para separar a lista de autores de uma mesma música em linhas individuais e calcular o número de coautores $N$.
* **Camada Gold (Regras de Negócio e Valuation):** Aplica taxas de pagamento de royalties conforme o canal (Rádio vs. Streaming) divididas pelo número de autores ($1/N$):

$$\text{Valuation Obra (BRL)} = \frac{\text{Execuções} \times \text{Taxa Royalty}}{\text{Qtd Coautores}}$$

E calcula a nota do Lead Score (0 a 100) combinando o faturamento acumulado ($70\%$) e a quantidade de músicas no ranking ($30\%$):

$$\text{Lead Score} = \left( \frac{\ln(1 + \text{Valuation})}{\ln(1 + \text{Valuation}_{\max})} \times 70 \right) + \left( \min\left(\frac{\text{Qtd Hits}}{5}, 1\right) \times 30 \right)$$

---

### Evidências de Execução do Pipeline PySpark

#### 3.1. Camada Bronze: Ingestão dos Dados
Ingestão do arquivo bruto da Crowley, criação das derivações de canais (Spotify, Deezer e Rádio) e consolidação da tabela inicial.

<img width="2046" height="1044" alt="Camada_Bronze" src="https://github.com/user-attachments/assets/20af529f-e519-4c0c-91f7-813b484af63b" />

#### 3.2. Camada Silver: Tratamento de Qualidade, MDM e Rateio (1/N)
Limpeza de strings, unificação de grandes gravadoras/editoras e desmembramento fracionado dos coautores via `explode()`.

<img width="2184" height="1161" alt="Camada_Silver" src="https://github.com/user-attachments/assets/448c17f8-55b9-45f5-bc87-1933212f76c2" />

#### 3.3. Camada Gold: Valuation e Regras Financeiras
Aplicação das taxas diferenciadas de royalty por canal e cálculo do faturamento estimado por coautor ($1/N$).

<img width="2046" height="936" alt="Camada_Gold" src="https://github.com/user-attachments/assets/85c72dd9-9777-4ae0-b038-c24cf3b0d7b2" />

#### 3.4. Camada Gold: Cálculo de Lead Score e Salvamento no Catalog
Agregação das métricas comerciais, cálculo logarítmico do Lead Score e confirmação de gravação no Unity Catalog.

<img width="2682" height="1302" alt="Tabelas_Gold" src="https://github.com/user-attachments/assets/81671974-58a9-4897-8405-7f92bb2321b2" />

---

## <a id="4-catalogo"></a>4. Modelagem e Catálogo de Dados

A tabela abaixo descreve a estrutura dos dados salvos na camada Gold do pipeline.

| Coluna | Tipo | Regra de Negócio e Descrição |
| :--- | :--- | :--- |
| `Rank` | `INT` | Posição oficial no ranking de execuções da Crowley |
| `Musica_Clean` | `STRING` | Título da música limpo e padronizado (`UPPER` + `TRIM`) |
| `Artista_Clean` | `STRING` | Nome do cantor ou banda principal do fonograma |
| `Compositor_Nome` | `STRING` | Nome individual do compositor desmembrado via `explode()` |
| `Qtd_Coautores` | `INT` | Quantidade total de coautores na música (fator $1/N$) |
| `Plataforma` | `STRING` | Origem do dado (*Spotify*, *Deezer*, *Radio*) |
| `Execucoes` | `BIGINT` | Quantidade de execuções ou reproduções medidas no período |
| `Valuation_Obra_RS` | `DOUBLE` | Faturamento estimado em Reais ($R\$$) referente à fração do autor |
| `Lead_Score` | `DOUBLE` | Nota de atratividade comercial para prospecção de A&R (0 a 100) |
| `Status_Editora` | `STRING` | Situação do autor (*SEM EDITORA*, *AUTOADMINISTRADO*, *EDITORA ESTABELECIDA*) |

---

## <a id="5-analise"></a>5. Análise de Dados e Resposta às Perguntas de Negócio

Para analisar os resultados da camada Gold, montei os gráficos diretamente em Python usando as bibliotecas `matplotlib` e `seaborn`. O painel foi dividido em quatro partes para mostrar diferentes visões dos dados:

1. **Top Compositores por Valuation (Canto Superior Esquerdo):** Mostra os autores com maior faturamento estimado no período. É nessa visão que encontramos os compositores independentes com mais dinheiro retido a receber.
2. **Participação de Mercado / Market Share (Canto Superior Direito):** Compara o volume de dinheiro retido pelas grandes editoras (Warner, Universal, Sony) em relação ao valor que pertence aos autores sem editora.
3. **Matriz de Priorização - Valuation vs. Lead Score (Canto Inferior Esquerdo):** Cruza o faturamento estimado em Reais com a nota do Lead Score para apontar os compositores que mais compensa buscar no mercado.
4. **Resumo das Obras (Canto Inferior Direito):** Exibe métricas do mercado, como o percentual de músicas feitas em coautoria múltipla, confirmando a importância da regra de divisão ($1/N$).

---

### Evidência 4: Painel Visual de Resultados

<img width="1645" height="938" alt="dashboard mvp engenharia de dados" src="https://github.com/user-attachments/assets/66e11317-35e2-4d13-a802-c4adf8245992" />

### Análise dos Resultados e Resposta às Perguntas:

1. **Oportunidades de Captação (A&R):**
   * **Confirmada.** O pipeline identificou compositores independentes no *Top Charts* gerando receita alta sem vínculo com editoras. Autores como **Marco Esteves** (R$ 384.801 a receber) e **Elvis Elan** (R$ 317.156) aparecem no topo da lista para contratação.
2. **Importância da Coautoria Múltipla ($1/N$):**
   * **Confirmada.** **68,3% das músicas** do ranking têm 3 ou mais compositores. Se o cálculo de divisão fracionada não fosse feito no PySpark, o pagamento seria calculado com valores errados.
3. **Concentração de Mercado (Market Share):**
   * A editora **Warner Chappell** fica na liderança com **21,9%** do dinheiro do mercado, seguida pela Universal Music (11,1%) e Sony Music Publishing (9,1%). A soma dos autores que operam sem editora representa **5,2%** de todo o valor movimentado.

---

---

## <a id="6-consideracoes"></a>6. Considerações Finais, Premissas e Pontos de Melhoria

### Considerações Finais e Premissas de Modelagem
O desenvolvimento deste **MVP** comprovou a viabilidade de resolver gargalos do mercado da música por meio de Engenharia de Dados. Para a estruturação do modelo no Databricks Serverless, foram adotadas as seguintes premissas de negócio e engenharia:

1. **Rateio Fracionado ($1/N$):** Como os relatórios brutos de auditoria de mercado (*Crowley Charts*) fornecem a relação de compositores sem discriminar os percentuais contratuais individuais (mantidos em sigilo pelo ECAD/Editoras), o pipeline adota a divisão igualitária proporcional ($1/N$) como a melhor aproximação estocástica para evitar a duplicação de receitas no faturamento.
2. **Normalização Financeira (Escala Logarítmica):** A distribuição de faturamento no mercado musical segue a lei de potência (Cauda Longa). O uso da transformação logarítmica ($\ln(1 + \text{Valuation}$) no algoritmo de *Lead Score* foi essencial para evitar que hits atípicos distorcessem a escala de pontuação (0 a 100).
3. **Modelagem de Fontes Omnichannel:** A alíquota diferenciada entre Rádio ($R\$$ 3,50 por execução) e Streaming Digital ($R\$$ 0,012 por stream) reflete a assimetria real de arrecadação entre direitos de Execução Pública (ECAD) e reproduções em plataformas sob demanda.

### Mapeamento de Limitações e Trabalhos Futuros:
1. **Contratos e Percentuais Dinâmicos:** Evoluir o pipeline para cruzar a Camada Silver com uma tabela relacional de contratos individuais, permitindo divisões percentuais assimétricas ($70\%/30\%$, por exemplo).
2. **Orquestração Automática de Jobs:** Em um ambiente de produção comercial, migrar da execução manual para um fluxo agendado e monitorado via **Databricks Workflows** ou **Azure Data Factory**.
3. **Ingestão Contínua (Streaming):** Evoluir a Camada Bronze para processar cargas em tempo real (*Structured Streaming*), consumindo APIs diretas do Spotify Web API e YouTube Content ID.
4. **Modelagem Preditiva Avançada:** Expandir o algoritmo de *Lead Score* na Camada Gold com modelos de Machine Learning (como *XGBoost*) para prever o potencial de estouro (*hit potential*) de novos lançamentos antes da consolidação do mercado.

---

## <a id="7-autoavaliacao"></a>7. Autoavaliação
Desenvolver este MVP foi uma experiência de enorme aprendizado técnico e visão de produto. No início da sprint, encarar o volume de dados e construir a arquitetura do zero no Databricks parecia um desafio intimidador. No entanto, decidir encarar um problema real de negócio me motivou a ir muito além do básico da teoria.

Destaco como principais pontos da minha trajetória neste trabalho:

* **Prática em PySpark:** Aprendi a aplicar rotinas de tratamento de dados, como limpeza de textos e o uso da função `explode()` para separar listas de autores, garantindo a organização das tabelas no **Unity Catalog**.
* **Entendimento do Problema:** Percebi que a engenharia de dados precisa fazer sentido para quem vai usar as informações. Definir as regras de pagamento de royalties e a nota de prioridade ajudou a transformar dados brutos em respostas úteis.
* **Autonomia e Confiança Técnica:** Construir a solução desde a carga do arquivo até a visualização gráfica trouxe a segurança de que consigo planejar e entregar um projeto de dados do início ao fim.
