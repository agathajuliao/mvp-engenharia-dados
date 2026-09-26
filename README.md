# MVP de Engenharia de Dados: Plataforma Omnichannel de Valuation e Inteligência de A&R Musical

## Sumário
1. [Contexto de Negócios e Perguntas](#1-contexto-de-negócios-e-perguntas)
2. [Carga dos Dados](#2-carga-dos-dados)
3. [Modelagem e Catálogo de Dados](#3-modelagem-e-catálogo-de-dados)
4. [Pipeline de Dados](#4-pipeline-de-dados)
5. [Qualidade de Dados](#5-qualidade-de-dados)
6. [Análise de Dados](#6-análise-de-dados)
7. [Autoavaliação](#7-autoavaliação)

---

## <a id="1-contexto-de-negócios-e-perguntas"></a>1. Contexto de Negócios e Perguntas

### Contexto do Problema de Negócio
No mercado da música, o consumo de faixas ocorre de forma dividida entre canais digitais (como Spotify e Deezer) e execuções nas rádios (*Airplay*). Essa divisão dificulta o acompanhamento de quem deve receber os direitos autorais por cada reprodução, gerando perdas financeiras (*royalty leakage*).

O problema principal acontece na **coautoria múltipla ($1/N$)**. As músicas mais tocadas nos rankings raramente são feitas por uma única pessoa; em geral, são escritas por grupos de três ou mais compositores. Quando um desses autores é independente (não tem contrato com grandes editoras) ou possui um cadastro com dados incompletos, a fatia de dinheiro que cabe a ele acaba retida em associações ou se perde pela falta de padronização nos registros.

Este projeto desenvolve um **Produto de Dados** no **Databricks Serverless** com **Unity Catalog** para resolver esse problema em três frentes:
1. **Padronização de Cadastro (Master Data Management - MDM):** Limpar e organizar os dados brutos da *Crowley Charts*, unificando nomes de grandes editoras (como Warner, Sony e Universal) para evitar duplicidades.
2. **Rateio de Coautoria ($1/N$):** Usar o PySpark para separar os nomes de todos os compositores de cada música e calcular a divisão exata e proporcional do dinheiro para cada um.
3. **Valuation e Priorização de Captação (Lead Score):** Calcular quanto dinheiro cada autor independente tem a receber e gerar uma nota de atratividade (0 a 100) para ajudar a equipe de A&R (*Artist & Repertoire*) a encontrar os compositores mais promissores para contratar.

### Resumo da Estrutura dos Dados Brutos
Os dados brutos de entrada consistem em relatórios de auditoria de mercado em formato CSV disponibilizados pela Crowley. A estrutura dos atributos de origem engloba:
* `Rank`: Posição oficial ocupada no ranking de audição.
* `Música`: Título da obra musical registrada.
* `Artista`: Intérprete ou grupo principal do fonograma.
* `Autor`: Relação textual de compositores e detentores dos direitos autorais.
* `Gravadora` / `Editora`: Entidade responsável pela administração e distribuição da obra.
* `Streams`: Volume absoluto de reproduções ou audiência estimada no período.

### Perguntas de Negócio (Hipóteses Estratégicas)
* **Pergunta 1 (Oceano Azul A&R):** Existem compositores no Top 200 da Crowley sem gestão editorial gerando valores expressivos ($> R\$$ 100 mil) em royalties de autoria retidos?
* **Pergunta 2 (Rateio por Coautoria):** Qual a proporção de obras do ranking que dependem de coautoria múltipla (3+ autores), exigindo o cálculo fracionado ($1/N$)?
* **Pergunta 3 (Concentração de Mercado):** Qual o nível de concentração de *Market Share* das grandes editoras frente aos autores autoadministrados?
* **Pergunta 4 (Assimetria Omnichannel):** A distribuição de consumo difere entre o *Streaming Digital* e o *Airplay de Rádio*?
* **Pergunta 5 (Retenção por Duplicidade):** A padronização de metadados permite identificar divergências cadastrais que geram retenção de receitas em associações?

> **Licença e Origem dos Dados:** Os dados brutos foram extraídos dos relatórios públicos de auditoria de mercado disponibilizados pela [Crowley Charts](https://charts.crowley.com.br/). A utilização deste conjunto de dados possui finalidade estritamente acadêmica e de pesquisa em Engenharia de Dados, sem fins comerciais.

---

## <a id="2-carga-dos-dados"></a>2. Carga dos Dados

### Explicação do Processo de Carga
Os arquivos em formato CSV com os dados oficiais da Crowley foram carregados diretamente no repositório de arquivos gerenciado em nuvem do **Databricks Volumes**, localizado no caminho `/Volumes/workspace/default/meus_arquivos/`.

A ingestão é executada via PySpark aplicando *bypass* das linhas de cabeçalho proprietárias do arquivo de origem (`skiprows=4`). O pipeline realiza a leitura em lote (*batch*), efetua a unificação dos esquemas dos canais de distribuição (Spotify, Deezer e Rádio) e aplica a conversão estruturada dos tipos de dados primitivos.

### Referência aos Scripts no GitHub
Todo o código responsável pela carga e processamento do pipeline está disponível na raiz deste repositório público:
* Script Python compilado: [`MVP_Engenharia de Dados.py`](./MVP_Engenharia%20de%20Dados.py)
* Notebook executável do Databricks: [`MVP_Engenharia de Dados.ipynb`](./MVP_Engenharia%20de%20Dados.ipynb)
* Exportação estática em HTML: [`MVP_Engenharia de Dados.html`](./MVP_Engenharia%20de%20Dados.html)

### Evidência 1: Carga dos Dados de Origem (Databricks Volumes)
Confirmação do armazenamento dos arquivos CSV de origem na estrutura de Volumes do ambiente de nuvem do Databricks.

<img width="3153" height="1647" alt="Databricks Volumes" src="https://github.com/user-attachments/assets/5bcff0ba-a7b8-4f22-afdd-0294560cc254" />

---

## <a id="3-modelagem-e-catálogo-de-dados"></a>3. Modelagem e Catálogo de Dados

### Explicação da Modelagem de Dados
O projeto adota o padrão de **Arquitetura Medallion** (organizada em camadas Bronze, Silver e Gold). A persistência e governança das tabelas são mantidas no **Unity Catalog** utilizando o formato aberto **Delta Lake** sob o esquema `workspace.default`. Esta estrutura garante suporte a propriedades ACID, versionamento histórico de dados (*Time Travel*) e alta eficiência analítica.
```
[ Databricks Volumes ] ──> [ Bronze: Ingestão Bruta ] ──> [ Silver: MDM & Split 1/N ] ──> [ Gold: Valuation & Lead Score ] ──> [ Dashboard Executivo ]
```
### Catálogo de Dados Transcrito (Camada Gold)

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

### Evidência 2: Estrutura das Tabelas no Unity Catalog
Screenshots do sistema de catálogo confirmando a criação e o registro das tabelas Delta nas camadas Medallion.

<img width="1029" height="834" alt="Unity Catalog" src="https://github.com/user-attachments/assets/c167fefc-3484-4d3d-af7a-abf8290c9d4f" />

---

## <a id="4-pipeline-de-dados"></a>4. Pipeline de Dados

### Organização do Processo de Pipeline ETL
O processo de extração, transformação e carga (ETL) foi centralizado e estruturado em um único notebook interativo em PySpark (`MVP_Engenharia de Dados.ipynb`), contendo a sequência completa de execução sem dependências manuais externas.

O fluxo de dados realiza a persistência e manutenção das tabelas Delta registradas no Unity Catalog e vinculadas aos scripts do repositório ([`.py`](./MVP_Engenharia%20de%20Dados.py) e [`.ipynb`](./MVP_Engenharia%20de%20Dados.ipynb)).

### Estágios do Processamento e Regras da Camada Gold
* **Camada Bronze (`bronze_omnichannel`):** Lê os arquivos CSV brutos armazenados nos Volumes, aplica o tratamento de cabeçalho e unifica os esquemas das plataformas.
* **Camada Silver (`silver_omnichannel`):** Executa o tratamento de qualidade, higienização de strings de editoras (MDM) e o desmembramento de coautoria fracionada via `F.explode()`.
* **Camada Gold (`gold_valuation_a_and_r` e `gold_concentracao_hhi`):** Aplica a precificação financeira dos royalties por canal dividida pelo fator de coautoria:

$$\text{Valuation Obra (BRL)} = \frac{\text{Execuções} \times \text{Taxa Royalty}}{\text{Qtd Coautores}}$$

E calcula a pontuação do algoritmo de atratividade comercial (*Lead Score*, de 0 a 100) combinando a escala logarítmica de faturamento acumulado ($70\%$) e a constância no ranking ($30\%$):

$$\text{Lead Score} = \left( \frac{\ln(1 + \text{Valuation})}{\ln(1 + \text{Valuation}_{\max})} \times 70 \right) + \left( \min\left(\frac{\text{Qtd Hits}}{5}, 1\right) \times 30 \right)$$

---

### Evidências de Execução do Pipeline e Tabelas Persistidas em Nuvem

#### 4.1. Camada Bronze: Ingestão dos Dados
Ingestão do arquivo bruto da Crowley, criação das derivações de canais (Spotify, Deezer e Rádio) e consolidação da tabela inicial.

<img width="2046" height="1044" alt="Camada Bronze" src="https://github.com/user-attachments/assets/20af529f-e519-4c0c-91f7-813b484af63b" />

#### 4.2. Camada Silver: Tratamento de Qualidade, MDM e Rateio (1/N)
Limpeza de strings, unificação de grandes gravadoras/editoras e desmembramento fracionado dos coautores via `explode()`.

<img width="2184" height="1161" alt="Camada Silver" src="https://github.com/user-attachments/assets/448c17f8-55b9-45f5-bc87-1933212f76c2" />

#### 4.3. Camada Gold: Valuation e Regras Financeiras
Aplicação das taxas diferenciadas de royalty por canal e cálculo do faturamento estimado por coautor ($1/N$).

<img width="2046" height="936" alt="Camada Gold Valuation" src="https://github.com/user-attachments/assets/85c72dd9-9777-4ae0-b038-c24cf3b0d7b2" />

#### 4.4. Camada Gold: Cálculo de Lead Score e Salvamento no Catalog
Agregação das métricas comerciais, cálculo logarítmico do Lead Score e confirmação de gravação no Unity Catalog.

<img width="2682" height="1302" alt="Camada Gold Lead Score" src="https://github.com/user-attachments/assets/81671974-58a9-4897-8405-7f92bb2321b2" />

---

## <a id="5-qualidade-de-dados"></a>5. Qualidade de Dados

### Problemas Detectados nos Dados Brutos
A etapa de exploração inicial identificou os seguintes gargalos de qualidade de dados:
* Linhas de cabeçalhos institucionais proprietários no arquivo CSV fonte.
* Nomes de editoras com ruídos de digitação, maiúsculas/minúsculas inconsistentes e variações cadastrais de um mesmo grupo econômico (ex: divergências na escrita de *Warner Chappell*, *Sony Music* e *Universal Music*).
* Presença de múltiplos compositores agrupados em um único campo de texto.
* Inexistência de um campo numérico indicando a quantidade de autores para o cálculo de divisão de receitas.

### Transformações e Soluções Aplicadas na Camada Silver
1. **Master Data Management (MDM):** Higienização de texto com as funções `UPPER` e `TRIM`, associada a regras condicionais (`F.when().contains()`) para unificar as variações de escrita das grandes editoras sob entidades padronizadas.
2. **Desmembramento Fracionado de Coautoria ($1/N$):** Conversão da coluna textual de autores em um vetor (`ArrayType`) via expressão regular com `F.split()`, seguida da expansão das linhas com a função `F.explode()` para associar cada autor a um registro próprio.
3. **Cálculo da Divisão Proporcional:** Apuração da coluna `Qtd_Coautores` via `F.size()` para aplicar o denominador exato $N$ no rateio da receita por obra.
4. **Remoção de Inconsistências:** Aplicação de filtros em PySpark para desconsiderar valores nulos ou campos de composição em branco após a explosão do vetor.

---

## <a id="6-análise-de-dados"></a>6. Análise de Dados

### Painel Visual de Resultados e Inteligência Analítica
Para analisar os dados salvos na camada Gold e responder aos questionamentos estratégicos, desenvolveu-se um painel gráfico programático em Python utilizando as bibliotecas `matplotlib` e `seaborn`.

### Evidência 4: Painel Visual de Resultados

<img width="1645" height="938" alt="Dashboard Executivo de Análise de Dados" src="https://github.com/user-attachments/assets/66e11317-35e2-4d13-a802-c4adf8245992" />

### Análise dos Resultados e Resposta às Perguntas de Negócio

1. **Resposta à Pergunta 1 (Oceano Azul de Captação A&R): CONFIRMADA.**
   * O pipeline identificou compositores independentes no *Top Charts* gerando receita elevada sem vínculo editorial. Autores como **Marco Esteves** (R$ 384.801 a receber) e **Elvis Elan** (R$ 317.156) aparecem no topo da lista de prioridade comercial.
2. **Resposta à Pergunta 2 (Relevância da Coautoria Múltipla $1/N$): CONFIRMADA.**
   * **68,3% das músicas** do ranking possuem 3 ou mais compositores. A divisão proporcional no PySpark evitou que o faturamento das faixas fosse duplicado ou atribuído incorretamente.
3. **Resposta à Pergunta 3 (Concentração de Mercado - Market Share): CONFIRMADA.**
   * A editora **Warner Chappell** detém a liderança com **21,9%** do faturamento do mercado, seguida pela Universal Music (11,1%) e Sony Music Publishing (9,1%). A soma dos autores atuando sem editora representa **5,2%** de toda a receita movimentada no período.
4. **Resposta à Pergunta 4 (Assimetria Omnichannel): CONFIRMADA.**
   * O consumo em Rádio gera repasses de Execução Pública (ECAD) com valor por reprodução muito superior às frações do streaming digital, demonstrando a importância de unificar as fontes de consumo no mesmo pipeline.
5. **Resposta à Pergunta 5 (Desbloqueio de Verbas Retidas): CONFIRMADA.**
   * A padronização de metadados via PySpark atuou como uma auditoria automática, permitindo identificar divergências de cadastro e criando condições para destravar pagamentos congelados nas associações.

### Premissas e Considerações de Modelagem
* **Rateio Fracionado ($1/N$):** Como os relatórios públicos da Crowley fornecem a relação de compositores sem especificar os percentuais contratuais individuais (mantidos em sigilo pelo ECAD/Editoras), o pipeline adota a divisão igualitária proporcional ($1/N$) como a melhor aproximação estocástica para evitar a duplicação de receitas no faturamento.
* **Escala Logarítmica no Lead Score:** A distribuição de faturamento no mercado musical segue a lei de potência (Cauda Longa). O uso da transformação logarítmica ($\ln(1 + \text{Valuation})$) no algoritmo de *Lead Score* evitou que hits atípicos distorcessem a escala de pontuação (0 a 100).
* **Modelagem de Fontes Omnichannel:** A alíquota diferenciada entre Rádio ($R\$$ 3,50 por execução) e Streaming Digital ($R\$$ 0,012 por stream) reflete a assimetria real de arrecadação do setor.

---

## <a id="7-autoavaliação"></a>7. Autoavaliação

Concluir este MVP representou uma jornada de aprendizado intenso e de consolidação prática em Engenharia de Dados. No início da sprint, encarar os dados desorganizados do mercado da música e estruturar do zero a arquitetura no Databricks parecia um desafio intimidador. No entanto, focar em um problema real de negócio foi o combustível para ir além da teoria e atingir todos os objetivos propostos: transformar relatórios brutos em um Produto de Dados funcional, transparente e útil.

### Dificuldades Encontradas e Aprendizados
Durante o desenvolvimento, enfrentei desafios práticos que exigiram bastante estudo, testes e atenção aos detalhes:
* **Manejo do PySpark e Divisão $1/N$:** A manipulação da função `F.explode()` para separar os coautores em linhas individuais trouxe o risco de multiplicar indevidamente o faturamento das músicas. Foi necessário entender a lógica do processamento distribuído para garantir que o rateio proporcional acontecesse sem inflar os valores totais.
* **Padronização de Metadados (MDM):** A inconsistência no cadastro das editoras nos relatórios brutos exigiu criar rotinas de limpeza de texto e agrupamento condicional, garantindo que grandes grupos como Warner, Universal e Sony fossem consolidados sob identidades únicas.
* **Ajustes Visuais do Painel Analítico:** Organizar os quatro gráficos programáticos em Python sem que os rótulos e nomes de compositores se sobrepusessem exigiu calibração cuidadosa da geometria das figuras e o uso da escala logarítmica para equilibrar os valores de faturamento.

### Trajetória e Visão de Produto
Essa experiência fortaleceu aspectos centrais da minha formação:
* **Orientação ao Valor de Negócio:** Percebi que a engenharia de dados só cumpre seu papel quando se conecta com a necessidade de quem vai usar a informação. Desenhar a regra financeira de royalties e a nota do *Lead Score* provou como transformar linhas de código em inteligência de mercado.
* **Autonomia e Confiança Técnica:** Construir uma solução completa, desde a carga do arquivo fonte no Volume até a geração do painel analítico, me deu a certeza de que consigo planejar, executar e entregar projetos de dados do início ao fim.

### Trabalhos Futuros e Evolução do Sistema
Como próximos passos para dar continuidade a este projeto no meu portfólio, pretendo:
1. **Suporte a Contratos Dinâmicos:** Cruzar a camada Silver com uma base relacional de contratos para permitir divisões percentuais assimétricas entre coautores ($70\%/30\%$, por exemplo).
2. **Orquestração Automática:** Configurar o agendamento dos notebooks via **Databricks Workflows** ou **Azure Data Factory** para execução periódica sem intervenção manual.
3. **Ingestão em Tempo Real:** Evoluir a Camada Bronze para processar cargas contínuas (*Structured Streaming*) consumindo APIs diretas do Spotify e YouTube Content ID.
4. **Modelagem Preditiva:** Expandir o algoritmo de *Lead Score* com modelos de Machine Learning para prever a tendência de viralização de novos lançamentos com base no histórico dos coautores.
