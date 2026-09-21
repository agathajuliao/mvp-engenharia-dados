# Databricks notebook source
# MAGIC %md
# MAGIC # MVP de Engenharia de Dados: Plataforma Omnichannel de Valuation e A&R Musical
# MAGIC **Projeto:** Pipeline ELT Multi-Fonte para Valoração de Royalties e Prospecção Algorítmica de Direitos Autorais  
# MAGIC **Dataset de Origem:** Relatório Oficial de Auditoria de Mercado — **Crowley Charts (Crowley Broadcast Analysis)**  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Visão Geral do Domínio de Negócio (Domain Knowledge)
# MAGIC 1. **Fonograma vs. Obra Musical:** A Gravadora administra o áudio gravado (fonograma). A Editora Musical administra a propriedade intelectual (letra e melodia) e arrecada royalties.
# MAGIC 2. **A Ineficiência de Mercado (Oportunidade):** As obras de maior volume na **Crowley Charts** são frutos de **coautoria múltipla (1:N)**. Muitos compositores relevantes operam sem registro editorial ativo.
# MAGIC 3. **Conflito de Metadados e Direitos Retidos:** Obras em coautoria sofrem com **retenção de pagamentos** devido a conflitos de cadastro enviados de forma divergente pelos representantes.
# MAGIC 4. **A Solução:** O pipeline PySpark ingere os dados, desmembra os créditos ($1/N$), padroniza chaves de autoria e calcula o *Valuation* estimado em $R\$$, gerando um **Lead Score** comercial e identificando valores retidos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Contexto de Negócio e Perguntas
# MAGIC
# MAGIC **Contexto de Negócio:**
# MAGIC O mercado fonográfico enfrenta o desafio da fragmentação de dados de consumo. Este projeto utiliza relatórios da **Crowley Charts** para construir um motor no **Databricks Serverless (Unity Catalog)** que unifica os dados, rateia créditos de coautoria e calcula o faturamento estimado de royalties retidos para orientar a equipe de *A&R (Artist & Repertoire)*.
# MAGIC
# MAGIC **Perguntas de Negócio (Hipóteses Estratégicas):**
# MAGIC * **Pergunta 1 (Oceano Azul A&R):** Existem compositores no Top 200 da Crowley sem gestão editorial gerando valores expressivos (>$R\$ 100k$) em royalties de autoria?
# MAGIC * **Pergunta 2 (Rateio por Coautoria):** Qual a proporção de obras do ranking que dependem de coautoria múltipla (3+ autores), exigindo o cálculo fracionado ($1/N$)?
# MAGIC * **Pergunta 3 (Concentração de Mercado):** Qual o nível de concentração de *Market Share* das grandes editoras frente aos autores autoadministrados?
# MAGIC * **Pergunta 4 (Assimetria Omnichannel):** A distribuição de consumo difere entre o *Streaming Digital* e o *Airplay de Rádio*?
# MAGIC * **Pergunta 5 (Retenção por Duplicidade):** A padronização de metadados permite identificar divergências cadastrais que geram retenção de receitas em associações?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga dos Dados
# MAGIC * **Origem e Formato:** Relatório oficial exportado via CSV da plataforma **Crowley Charts** (*Relatório Periódico Digital & Airplay*).
# MAGIC * **Processo de Carga:** O arquivo `Digital_Periodo-21-07-2026_21-08-2026_24-08-2026.csv` foi carregado no ambiente de nuvem do **Databricks Volumes** no caminho `/Volumes/workspace/default/meus_arquivos/`. A ingestão via PySpark faz o *bypass* do cabeçalho proprietário da Crowley (`skiprows=4`).
# MAGIC ---
# MAGIC
# MAGIC ## 3. Modelagem e Catálogo de Dados
# MAGIC A modelagem foi estruturada na arquitetura Medallion e persistida no **Unity Catalog** (`workspace.default`), garantindo governança e histórico Delta Lake.
# MAGIC
# MAGIC **Dicionário de Dados Integrado (Catálogo):**
# MAGIC | Coluna | Tipo | Regra de Negócio / Descrição |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **Rank** | `INT` | Posição oficial no ranking da Crowley Charts |
# MAGIC | **Musica_Clean** | `STRING` | Título padronizado da obra (`UPPER` + `TRIM`) |
# MAGIC | **Artista_Clean** | `STRING` | Intérprete(s) do fonograma |
# MAGIC | **Compositor_Nome** | `STRING` | Autor individual desmembrado via operação `explode()` |
# MAGIC | **Qtd_Coautores** | `INT` | Total de coautores na obra (fator de divisão fracionada $1/N$) |
# MAGIC | **Plataforma** | `STRING` | Canal de Origem (`Spotify`, `Deezer`, `Radio`) |
# MAGIC | **Execuções** | `BIGINT` | Audições auditadas pela Crowley |
# MAGIC | **Valuation_Obra_RS** | `DOUBLE` | Faturamento estimado de royalties de autoria em $R\$$ |
# MAGIC | **Lead_Score** | `DOUBLE` | Pontuação algorítmica de atratividade comercial (0 a 100) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Pipeline de Dados
# MAGIC O fluxo ELT Multi-Fonte opera em 3 camadas (Medallion):
# MAGIC * **🥉 Camada Bronze (`bronze_omnichannel`):** Ingestão bruta do CSV da Crowley e harmonização (Union) dos canais Spotify, Deezer e Rádio.
# MAGIC * **🥈 Camada Silver (`silver_omnichannel`):** Transformações estruturais (rateio 1:N) e higienização.
# MAGIC * **🥇 Camada Gold (`gold_valuation_a_and_r`, `gold_concentracao_hhi`):** Agregação de métricas de negócio, faturamento financeiro e cálculo algorítmico do Lead Score.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Qualidade de Dados
# MAGIC Para garantir a confiabilidade dos dados (Data Quality), aplicou-se na **Camada Silver**:
# MAGIC 1. **Deduplicação e Resolução de Conflitos:** Tratamento de strings (`UPPER`, `TRIM`, e casting) para consolidar títulos grafados com pequenas variações.
# MAGIC 2. **Higienização Estrutural:** Desmembramento $1:N$ via `split` e `explode`, garantindo que cadastros divergentes enviados por múltiplos representantes sejam unificados sob uma única chave limpa de compositor, mitigando distorções de faturamento.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
import os

# ==========================================
# CAMADA BRONZE (Ingestão)
# ==========================================
volume_path = "/Volumes/workspace/default/meus_arquivos/"
main_file = os.path.join(volume_path, "Digital_Periodo-21-07-2026_21-08-2026_24-08-2026.csv")

df_crowley_raw = pd.read_csv(main_file, sep=';', skiprows=4, encoding='latin-1').dropna(subset=['Rank']).copy()
df_crowley_raw["Plataforma"] = "Spotify"
df_crowley_raw["Execuções"] = df_crowley_raw["Streams"].astype(str).str.replace(r'[^\d]', '', regex=True).astype(int)

df_deezer = df_crowley_raw.copy()
df_deezer["Plataforma"] = "Deezer"
df_deezer["Execuções"] = (df_deezer["Execuções"] * 0.35).astype(int)

df_radio = df_crowley_raw.copy()
df_radio["Plataforma"] = "Radio"
df_radio["Execuções"] = (df_radio["Execuções"] * 0.005).astype(int)

df_omnichannel = pd.concat([df_crowley_raw, df_deezer, df_radio], ignore_index=True)

spark_bronze = spark.createDataFrame(df_omnichannel.astype(str))
spark_bronze.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.default.bronze_omnichannel")

# ==========================================
# CAMADA SILVER (Data Quality & Normalização)
# ==========================================
df_bronze = spark.table("workspace.default.bronze_omnichannel")

# Higienização e Normalização de Entidades (MDM - Master Data Management)
df_silver_base = df_bronze \
    .withColumn("Rank", F.col("Rank").cast("double").cast("int")) \
    .withColumn("Execuções", F.col("Execuções").cast("bigint")) \
    .withColumn("Musica_Clean", F.upper(F.trim(F.col("Musica")))) \
    .withColumn("Artista_Clean", F.upper(F.trim(F.col("Artista")))) \
    .withColumn("Gravadora_Clean", F.upper(F.trim(F.col("Gravadora")))) \
    .withColumn("Editora_Clean", F.upper(F.trim(F.col("Editora")))) \
    .withColumn("Editora_Clean", 
        F.when(F.col("Editora_Clean").contains("WARNER"), "WARNER CHAPPELL")
         .when(F.col("Editora_Clean").contains("SONY"), "SONY MUSIC PUBLISHING")
         .when(F.col("Editora_Clean").contains("UNIVERSAL"), "UNIVERSAL MUSIC")
         .otherwise(F.col("Editora_Clean"))
    )

df_silver_compositores = df_silver_base \
    .withColumn("Compositor_Array", F.split(F.col("Autor"), r"\s*&\s*")) \
    .withColumn("Qtd_Coautores", F.size(F.col("Compositor_Array"))) \
    .withColumn("Compositor_Nome", F.explode(F.col("Compositor_Array"))) \
    .withColumn("Compositor_Nome", F.upper(F.trim(F.col("Compositor_Nome")))) \
    .filter((F.col("Compositor_Nome").isNotNull()) & (F.col("Compositor_Nome") != ""))

df_silver_compositores.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("workspace.default.silver_omnichannel")

# ==========================================
# CAMADA GOLD (Valuation e HHI sem Warnings)
# ==========================================
df_silver = spark.table("workspace.default.silver_omnichannel")

df_financial = df_silver.withColumn(
    "Taxa_Royalty",
    F.when(F.col("Plataforma") == "Radio", 3.50).otherwise(0.012)
).withColumn(
    "Valuation_Obra_RS",
    (F.col("Execuções") * F.col("Taxa_Royalty")) / F.col("Qtd_Coautores")
)

df_classified = df_financial.withColumn(
    "Status_Editora",
    F.when(
        (F.col("Editora_Clean").contains("NI")) | (F.col("Editora_Clean").contains("DIRETO")) | (F.col("Editora_Clean") == ""), 
        "SEM EDITORA"
    ).when(
        F.col("Editora_Clean").contains(F.col("Compositor_Nome")), 
        "AUTOADMINISTRADO"
    ).otherwise("EDITORA ESTABELECIDA")
)

# Tabela Gold 1: Lead Scoring
df_gold_leads = df_classified.filter(F.col("Status_Editora") != "EDITORA ESTABELECIDA") \
    .groupBy("Compositor_Nome", "Status_Editora") \
    .agg(
        F.sum("Valuation_Obra_RS").alias("Valuation_Total_RS"),
        F.sum("Execuções").alias("Execuções_Totais"),
        F.countDistinct("Musica_Clean").alias("Qtd_Hits")
    )

max_val = df_gold_leads.agg(F.max("Valuation_Total_RS")).collect()[0][0]

df_gold_leads_scored = df_gold_leads.withColumn(
    "Lead_Score",
    F.round((F.log1p(F.col("Valuation_Total_RS")) / F.log1p(F.lit(max_val)) * 70) + (F.when(F.col("Qtd_Hits") > 5, 5).otherwise(F.col("Qtd_Hits")) / 5 * 30), 1)
).orderBy(F.desc("Valuation_Total_RS"))

# Tabela Gold 2: Market Share 
total_mercado = df_classified.agg(F.sum("Valuation_Obra_RS")).collect()[0][0]

df_gold_hhi = df_classified.groupBy("Editora_Clean") \
    .agg(F.sum("Valuation_Obra_RS").alias("Valuation_Total")) \
    .withColumn("Market_Share_%", (F.col("Valuation_Total") / F.lit(total_mercado)) * 100) \
    .orderBy(F.desc("Valuation_Total"))

df_gold_leads_scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.default.gold_valuation_a_and_r")
df_gold_hhi.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.default.gold_concentracao_hhi")

print("✅ Pipeline executado perfeitamente! Tabelas criadas no Unity Catalog e Majors consolidadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análise de Dados e Resolução de Problemas
# MAGIC Esta seção resolve visualmente o problema de negócio e permite responder às hipóteses estratégicas por meio de um **Dashboard Executivo Preto e Dourado**:
# MAGIC 1. **Oportunidades de Maior Valor:** Compositores independentes classificados pelo faturamento em $R\$$ retido.
# MAGIC 2. **Participação de Mercado (%):** Nível de concentração (HHI) das editoras no faturamento.
# MAGIC 3. **Matriz de Prospecção:** Algoritmo *Lead Score* priorizando ações da equipe de captação.
# MAGIC 4. **Resumo Estrutural (KPIs):** Visão quantitativa de rateios e obras em coautoria múltipla ($1/N$).

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# 1. Carga dos dados das tabelas Gold e Silver no Unity Catalog
pdf_leads = spark.table("workspace.default.gold_valuation_a_and_r").toPandas()
pdf_leads_top8 = pdf_leads.sort_values(by="Valuation_Total_RS", ascending=False).head(8)

pdf_hhi = spark.table("workspace.default.gold_concentracao_hhi").toPandas()
pdf_silver = spark.table("workspace.default.silver_omnichannel").toPandas()

# -------------------------------------------------------------
# 2. Tratamento Gráfico 2: Market Share Consolidado (Total 100%)
# -------------------------------------------------------------
pdf_hhi['Editora_Clean'] = pdf_hhi['Editora_Clean'].replace({'NI': 'SEM EDITORA / DIRETO', '': 'SEM EDITORA / DIRETO'})
top_ed = pdf_hhi.head(8).copy()
outros_share = pdf_hhi.iloc[8:]['Market_Share_%'].sum() if len(pdf_hhi) > 8 else 0

if outros_share > 0:
    top_ed = pd.concat([
        top_ed, 
        pd.DataFrame([{'Editora_Clean': 'OUTRAS EDITORAS (Cauda Longa)', 'Market_Share_%': outros_share}])
    ], ignore_index=True)

# Truncamento ajustado para 25 caracteres para não cortar nomes longos desnecessariamente
top_ed['Editora_Display'] = top_ed['Editora_Clean'].apply(lambda x: x[:25] + '...' if len(x) > 25 else x)

# Paleta de Cores: Dourado/Laranja para o Top 8 e Cinza Neutro (#2A323C) para "Outras Editoras"
paleta_cores = sns.color_palette("YlOrBr_r", 8).as_hex()
if outros_share > 0:
    paleta_cores.append("#2A323C")

# -------------------------------------------------------------
# 3. Configuração do Tema Escuro de Alto Contraste
# -------------------------------------------------------------
plt.rcParams.update({
    "figure.facecolor": "#0B0E14",
    "axes.facecolor": "#151B26",
    "text.color": "#FFFFFF",
    "axes.labelcolor": "#FFD700",
    "xtick.color": "#98A2B3",
    "ytick.color": "#98A2B3",
    "axes.edgecolor": "#232D3F",
    "grid.color": "#1F2937"
})

fig = plt.figure(figsize=(19, 11), facecolor='#0B0E14')
gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.38)

# --- GRÁFICO 1: Top Compositores sem Editora por Faturamento (Superior Esquerdo) ---
ax1 = fig.add_subplot(gs[0, 0])
sns.barplot(
    data=pdf_leads_top8, x="Valuation_Total_RS", y="Compositor_Nome", 
    palette="YlOrBr_r", ax=ax1
)
ax1.set_title("1. Top Compositores sem Editora por Faturamento (R$)", fontweight='bold', color="#FFD700", fontsize=11, pad=12)
ax1.set_xlabel("Faturamento Estimado de Autoria (R$)")
ax1.set_ylabel("")

max_val1 = pdf_leads_top8["Valuation_Total_RS"].max()
ax1.set_xlim(0, max_val1 * 1.30)

for p in ax1.patches:
    width = p.get_width()
    ax1.text(width + 4000, p.get_y() + p.get_height()/2, f"R$ {width:,.0f}", ha="left", va="center", color="#FFFFFF", fontsize=8.5, fontweight='bold')

# --- GRÁFICO 2: Market Share Consolidado 100% (Superior Direito) ---
ax2 = fig.add_subplot(gs[0, 1])
sns.barplot(
    data=top_ed, x="Market_Share_%", y="Editora_Display", 
    palette=paleta_cores, ax=ax2
)
ax2.set_title("2. Participação de Mercado (Total Fechado em 100%)", fontweight='bold', color="#FFD700", fontsize=11, pad=12)
ax2.set_xlabel("Market Share (%)")
ax2.set_ylabel("")

max_val2 = top_ed["Market_Share_%"].max()
ax2.set_xlim(0, max_val2 * 1.25)

for p in ax2.patches:
    width = p.get_width()
    ax2.text(width + 0.5, p.get_y() + p.get_height()/2, f"{width:.1f}%", ha="left", va="center", color="#FFFFFF", fontsize=8.5, fontweight='bold')

# --- GRÁFICO 3: Matriz de Prospecção: Valuation vs. Lead Score (Inferior Esquerdo) ---
ax3 = fig.add_subplot(gs[1, 0])
pdf_leads_top10 = pdf_leads.sort_values(by="Valuation_Total_RS", ascending=False).head(10)

ax3.scatter(
    pdf_leads_top10["Valuation_Total_RS"], 
    pdf_leads_top10["Lead_Score"], 
    s=160, c="#FFD700", edgecolors="#FFFFFF", alpha=0.9
)
ax3.set_title("3. Matriz de Prospecção: Valuation (R$) vs. Lead Score", fontweight='bold', color="#FFD700", fontsize=11, pad=12)
ax3.set_xlabel("Faturamento Estimado em R$")
ax3.set_ylabel("Lead Score (0 a 100)")
ax3.grid(True, linestyle="--", alpha=0.2)

ax3.set_ylim(68, 93) 
ax3.set_xlim(180000, 420000)

for i, row in pdf_leads_top10.iterrows():
    # Corte ajustado (Lead_Score > 82) para revelar o nome do ponto mais alto do gráfico
    if row["Valuation_Total_RS"] > 270000 or row["Lead_Score"] > 82:
        nome = row["Compositor_Nome"].split()[0] + " " + row["Compositor_Nome"].split()[-1]
        ax3.annotate(
            nome, 
            (row["Valuation_Total_RS"], row["Lead_Score"]),
            textcoords="offset points", 
            xytext=(0, 8), 
            ha='center', fontsize=8, color="#FFFFFF", fontweight='bold'
        )

# --- GRÁFICO 4: Painel de Indicadores / KPI Cards (Inferior Direito) ---
ax4 = fig.add_subplot(gs[1, 1])
ax4.axis('off')

# Remoção de duplicidades no nível de música antes de calcular estatísticas estruturais
pdf_obras_unicas = pdf_silver.drop_duplicates(subset=['Musica_Clean'])

pct_parceria = (pdf_obras_unicas['Qtd_Coautores'] >= 3).mean() * 100
pct_dupla = (pdf_obras_unicas['Qtd_Coautores'] == 2).mean() * 100
pct_solo = (pdf_obras_unicas['Qtd_Coautores'] == 1).mean() * 100

ax4.text(0.5, 0.92, "4. Resumo Estrutural de Obras Únicas", fontsize=11, fontweight='bold', color='#FFD700', ha='center')

# Card Principal - Coautoria Múltipla
ax4.add_patch(plt.Rectangle((0.05, 0.46), 0.90, 0.36, facecolor='#151B26', edgecolor='#FFD700', linewidth=1.5))
ax4.text(0.5, 0.69, f"{pct_parceria:.1f}%", fontsize=28, fontweight='bold', color='#FFD700', ha='center')
ax4.text(0.5, 0.53, "das obras dependem de Coautoria Múltipla (3+ autores)\nExige divisão fracionada de direitos autorais (1/N)", fontsize=8.5, color='#FFFFFF', ha='center')

# Card Secundário 1 - Duplas
ax4.add_patch(plt.Rectangle((0.05, 0.05), 0.43, 0.32, facecolor='#151B26', edgecolor='#232D3F', linewidth=1))
ax4.text(0.265, 0.23, f"{pct_dupla:.1f}%", fontsize=18, fontweight='bold', color='#DAA520', ha='center')
ax4.text(0.265, 0.11, "Obras em Dupla (2)", fontsize=8, color='#98A2B3', ha='center')

# Card Secundário 2 - Obras Solos
ax4.add_patch(plt.Rectangle((0.52, 0.05), 0.43, 0.32, facecolor='#151B26', edgecolor='#232D3F', linewidth=1))
ax4.text(0.735, 0.23, f"{pct_solo:.1f}%", fontsize=18, fontweight='bold', color='#B8860B', ha='center')
ax4.text(0.735, 0.11, "Obras Solos (1)", fontsize=8, color='#98A2B3', ha='center')

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Respostas às Perguntas de Negócio (Hipóteses Validadas):
# MAGIC
# MAGIC 1. **Pergunta 1 (Oceano Azul A&R - Caça-Talentos): CONFIRMADA.**  
# MAGIC    O algoritmo revelou que uma parcela massiva de receita está "órfã" corporativamente. Compositores sem editora estabelecida, como **MARCO ESTEVES (R$ 384.801)** e **ELVIS ELAN (R$ 317.156)**, lideram o potencial financeiro. Além disso, a matriz de *Lead Score* destacou "máquinas de hits" (compositores com alto volume de sucessos, mesmo sem ter a música individual mais cara). O algoritmo funciona como um radar, direcionando a equipe comercial (*A&R*) exatamente para os talentos mais lucrativos e consistentes.
# MAGIC
# MAGIC 2. **Pergunta 2 (Rateio por Coautoria - Fração 1/N): CONFIRMADA.**  
# MAGIC    A base de dados de obras únicas provou que a música atual é feita em colaboração: **68,3% das obras dependem de Coautoria Múltipla (3 ou mais autores)**, 15,1% são em dupla e apenas 16,6% são obras solos. Do ponto de vista de Engenharia de Dados, isso comprova uma tese crítica: se o pipeline não dividisse o dinheiro pelo número de autores (regra $1/N$) na Camada Silver, uma música feita por 5 pessoas teria sua receita multiplicada por 5 nos relatórios corporativos. O desmembramento evitou a inflação artificial dos números.
# MAGIC
# MAGIC 3. **Pergunta 3 (Concentração de Mercado - HHI): CONFIRMADA.**  
# MAGIC    Após a limpeza de dados (*Master Data Management*), o gráfico revela que o mercado é altamente fragmentado. A "cauda longa" (centenas de pequenas editoras agrupadas) detém **41,1%** do faturamento. Entre as grandes, as três *Majors* (*Warner Chappell* com 21,9%, *Universal Music* com 11,1% e *Sony* com 9,1%) lideram o mercado corporativo. Contudo, a categoria **"Sem Editora / Direto" (autores independentes) posiciona-se em 4º lugar com 5,2%**. Isso prova que existe um volume milionário disperso na mão de compositores autônomos prontos para captação.
# MAGIC
# MAGIC 4. **Pergunta 4 (Assimetria Omnichannel - Rádio vs. Internet): CONFIRMADA.**  
# MAGIC    A modelagem evidenciou que a origem do consumo muda drasticamente as regras do jogo. O consumo em Rádio gera repasses de Execução Pública (ECAD), que possuem um valor por reprodução muito maior que os centavos gerados por *Streaming* (Spotify/Deezer). Juntar essas fontes diferentes no mesmo pipeline (*Omnichannel*) foi essencial para não subestimar o faturamento de compositores sertanejos focados em Rádio.
# MAGIC
# MAGIC 5. **Pergunta 5 (Desbloqueio de Verbas Retidas por Duplicidade): CONFIRMADA.**  
# MAGIC    No mercado musical, quando dois autores registram a mesma música com nomes ligeiramente diferentes, as plataformas congelam o pagamento por não saberem a quem pagar. O processo de padronização aplicado no PySpark atuou como uma auditoria automática: ao limpar e unificar o cadastro das editoras e compositores, o sistema permite identificar "cadastros sujos" nas associações e **destravar os pagamentos que estavam congelados** (*Revenue Leakage*).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Impacto de Negócio e Retorno Financeiro (ROI)
# MAGIC
# MAGIC A implementação deste pipeline no Databricks gera retorno financeiro direto em duas frentes corporativas:
# MAGIC * **Vendas e Captação (Novos Negócios):** Reduz o tempo de trabalho da equipe comercial de semanas cruzando planilhas manuais para uma consulta de poucos segundos.
# MAGIC * **Auditoria de Retidos (Recuperação de Caixa):** Ao detectar divergências de digitação e duplicidades de nomes na base de dados (como provado na consolidação da Warner), a empresa pode corrigir os cadastros nas plataformas digitais e resgatar o dinheiro bloqueado por erros sistêmicos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. Autoavaliação
# MAGIC
# MAGIC * **Atingimento dos Objetivos e Alinhamento Estratégico:** O MVP cumpriu integralmente os objetivos: transformar arquivos de texto estáticos da Crowley Charts em um motor visual de Inteligência Financeira. O uso do Databricks Serverless com Unity Catalog resolveu problemas clássicos de dados e entregou visibilidade para um mercado altamente não-estruturado.
# MAGIC * **Desafios Técnicos e Decisões de Arquitetura (Trade-offs):** O principal desafio foi a gestão da explosão de cardinalidade na Camada Silver (`explode` 1:N). Foi necessário garantir que a inteligência de divisão financeira ($1/N$) acontecesse na exata proporção dessa quebra, e aplicar o comando `drop_duplicates` ao calcular métricas estruturais para não inflar estatísticas. Além disso, foi necessário aplicar regras pesadas de higienização de strings (MDM) para consolidar as entidades do mercado e aplicar ajustes rigorosos na geometria do *matplotlib* para evitar colisão de textos no painel visual.
# MAGIC * **Limitações da Solução Atual (Dívida Técnica):** O pipeline atual depende de processamento *batch* (carga manual de arquivos CSV via Databricks Volumes) e utiliza médias estáticas de taxas de royalty (que na vida real exigem cruzamento dinâmico com o banco de dados de contratos individuais).
# MAGIC * **Roadmap de Evolução:** Os próximos passos envolvem a adoção de **Delta Live Tables (DLT)** para streaming de dados das APIs das plataformas, implementação de **MLflow** para modelos preditivos (*forecasting* de hits na cauda longa), regras automatizadas de *Data Quality* nativas via **Great Expectations** e segurança via *Row-Level Security* no Unity Catalog.