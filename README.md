# Pipeline PDTIC 2024-2027 — Databricks → Power BI

Monta as camadas de dados do Painel de Monitoramento do PDTIC a partir do
Redmine (MySQL), com consumo final pelo Power BI.

## Como a origem é lida

A extração é feita pelo **Pentaho**, que roda dentro da rede do MJSP e alcança
o banco. Ele gera um CSV por tabela e os envia ao DBFS; o Databricks lê esses
arquivos e monta as camadas.

```
Redmine (MySQL) → Pentaho → CSV no DBFS → bronze → silver → gold → Power BI
```

Não há leitura JDBC nem Lakehouse Federation: o compute do Databricks não tem
rota de rede até a origem, e habilitá-la exigiria migrar o banco de subnet
delegada para private endpoint, com janela de indisponibilidade. Por isso
nenhum notebook precisa de senha de banco, Vault ou secret scope.

## Ordem de execução

**Rotina mensal**, orquestrada pelo `99_run_all`:

| Notebook | Papel | Destino |
|---|---|---|
| `00_config_e_utils` | Parâmetros e funções comuns | — |
| `10_bronze_ingestao` | Lê os CSVs do Pentaho | `pdtic_bronze.*` |
| `20_silver_atributos` | Achata os campos de negócio (pivot) | `pdtic_silver.atributos` |
| `30_gold_necessidades` | Inventário, pontuação e ranking | `pdtic_gold.f_necessidades` |
| `31_gold_necessidade_meta_acao` | Lista de ações | `pdtic_gold.f_necessidade_meta_acao` |
| `32_gold_demandas` | Lista de demandas | `pdtic_gold.f_demandas` |
| `40_controle_atualizacao_pdtic` | Competência de referência e histórico de execuções | `pdtic_gold.controle_atualizacao_pdtic`, `historico_execucao_pdtic` |

**Fora da rotina**, executados quando a área de negócio fornece planilhas:

| Notebook | Papel | Destino |
|---|---|---|
| `90_carga_historico` | Carrega a série histórica | `pdtic_silver.h_*` |
| `91_promove_historico_gold` | Promove o histórico para a camada de consumo | `pdtic_gold.f_*` |

## O que o Power BI consome

Apenas a **gold**. As três tabelas de fato são particionadas por `extracao`, de
modo que o histórico é um filtro de mês, e não uma tabela à parte:

| Tabela | Página do painel |
|---|---|
| `f_necessidades` | Inventário de Necessidades e Priorização |
| `f_necessidade_meta_acao` | Lista de Ações |
| `f_demandas` | Lista de Demandas |

As tabelas `h_*` permanecem na silver como registro do que a área de negócio
entregou, e não são acessadas pelo painel. A coluna `_origem` distingue, na
gold, as linhas vindas de planilha das produzidas pelo pipeline.

## Agendamento

A extração no Pentaho roda **diariamente, à meia-noite** — não apenas no
último dia do mês. O objetivo é manter o arquivo do Data Lake sempre
atualizado com o dia corrente, de modo que uma atualização solicitada fora do
calendário mensal não precise acionar a extração manualmente: basta disparar
o processamento no Databricks, que consumirá o arquivo já extraído naquele
dia.

O processamento no Databricks e a atualização do Power BI seguem agendados
para o último dia do mês, compondo o fluxo automático mensal:

| Plataforma | Agendamento | Horário |
|---|---|---|
| Pentaho | cron diário do servidor de produção, chamando `executa_pdtic.sh` | 0h |
| Databricks | Job `atualização_carga_mensal_pdtic`, no último dia do mês | 1h |
| Power BI | Atualização agendada do painel, no último dia do mês | 2h |

No Databricks e no Power BI, o agendamento considera os dias 28 a 31 para
alcançar o último dia do mês. O job do Databricks tem **seis** tarefas
encadeadas, nesta ordem: `10` (bronze), `20` (silver), `30`, `31`, `32`
(gold) e `40` (controle de atualização — ver seção seguinte). O `99_run_all`
permanece para execução manual dessa cadeia. Como as tabelas de fato
reescrevem apenas a partição do mês corrente, execuções repetidas no mesmo
mês não duplicam registros: a mais recente substitui a anterior.

O cron do Pentaho está, por ora, no usuário do responsável pela implantação. A
troca por usuário de serviço permanece prevista.

### Atualização manual

Está em implantação um fluxo no **Power Automate** que aciona o
processamento do Databricks a qualquer momento do mês, independente do
agendamento automático — para subsidiar, por exemplo, reuniões do CGDSIC. A
extração diária do Pentaho torna esse disparo suficiente por si só: não é
necessário acionar a extração manualmente.

A atualização manual grava a mesma partição do mês corrente que o
processamento automático grava ao final do mês. A área de negócio foi
cientificada de que a atualização manual reflete a posição da base no
momento de sua execução, e que a execução automática do último dia do mês
prevalece como snapshot definitivo, podendo sobrescrever dados apresentados
em atualizações manuais anteriores caso a base tenha se alterado no
intervalo.

## Controle de atualização (notebook 40)

Último notebook da cadeia. Não produz dado para o painel: identifica a
competência (`extracao`) comum às três tabelas de fato, determina a data
oficial de atualização e grava duas tabelas de controle:

| Tabela | Grão | Conteúdo |
|---|---|---|
| `controle_atualizacao_pdtic` | Uma linha por competência | Última situação conhecida de cada mês: data de referência, origem (pipeline ou planilha histórica), quantidade de registros em cada fato |
| `historico_execucao_pdtic` | Uma linha por execução | Registro de cada rodada do job: início, fim, duração, status, quem/o que disparou |

A data oficial de atualização usa o maior `dt_consulta` da planilha quando
a competência vier de carga histórica, e o maior `dt_carga` do pipeline nos
demais casos. Como o notebook só grava `SUCESSO` ao ser alcançado, uma
falha em etapa anterior não gera registro de falha por conta própria — isso
cabe ao orquestrador externo (Power Automate), que deve consultar o
resultado do run no Databricks.

## Definições da área de negócio

- **Escopo:** somente necessidades vinculadas simultaneamente a uma Meta e a
  uma Ação. A lista fixa de identificadores do script original foi descartada.
- **Priorização:** apenas necessidades Em Monitoramento recebem posição no
  ranking. As em "Pendente Inclusão" ficam sem prioridade, mas permanecem no
  inventário e no gráfico de progresso.
- **Campos exibidos:** Subtipo e Portfólio passam a integrar o painel.
- **Histórico:** julho de 2026 é o marco inicial da série.
- **Período:** exibido no formato MM/AAAA.

## Composição da pontuação

| Coluna | O que agrega |
|---|---|
| `pts_a_criterios` | Exigência legal (100), projeto estratégico do Governo (75) e do MJSP (75), transformação digital (50), transversal (50) |
| `pts_b_manual` | Ministro, SE, CGDSIC, unidade demandante |
| `pts_total` | A + B |

As dez parcelas individuais continuam disponíveis, de modo que a soma é
auditável coluna a coluna. Os pesos ficam em `pdtic_silver.dim_pontuacao`:
alterá-los é um `UPDATE`, não mudança de código.

A priorização atribuída pela **CGE** permanece disponível no inventário para
consulta, mas não compõe a pontuação — mesmo comportamento do processo
anterior. O desempate entre necessidades de mesma pontuação observa, em
ordem, a data de criação e, remanescendo o empate, o identificador da
necessidade.

**Divergência conhecida:** o script original exibe o campo 127 (`prioriz_cge`)
mas não o inclui na totalização. Aqui ele entra em `pts_b_manual`. Hoje sem
efeito prático — o campo está zerado em todas as necessidades —, mas pendente
de confirmação com a área de negócio.

## Números de referência

Apurados na base de origem e usados como critério de aceitação.

| Ref. | Consulta | Agosto/2026 | Setembro/2026 |
|---|---|---|---|
| A | Necessidades com meta e ação | 150 | 154 |
| B | Necessidade→Meta→Ação | 340 linhas / 337 ações | 345 / 342 |
| C | Demandas | 111 | 111 |
| E | Remanejamentos de ciclo | 4.916 | — |

A base é viva: os números crescem com a movimentação natural. Em setembro
passou a aparecer o status **Suspensa** no escopo do inventário, que não
ocorria até julho.

## Como executar

### 1. ETL no Pentaho (servidor)

Gera os dez CSV e os envia ao DBFS. Conecte-se ao servidor pela porta 60001:

```bash
ssh -p 60001 <usuario>@10.10.12.111        # produção
ssh -p 60001 <usuario>@10.10.0.117         # homologação

/home/pentaho/projetos/pdtic/bat/executa_pdtic.sh
```

O script cuida do `JAVA_HOME`, do `KETTLE_HOME` e do log. Para rodar sem ele,
exporte as variáveis antes e redirecione a saída:

```bash
export JAVA_HOME=/home/pentaho/java/java-17-openjdk-17.0.15.0.6-2.el9.x86_64
export PATH=$JAVA_HOME/bin:$PATH
export KETTLE_HOME=/home/pentaho/projetos/pdtic
cd /home/pentaho/projetos/pdtic

/home/pentaho/pentaho_10.2.0.0-222/pdi-ce-10.2.0.0-222/data-integration/kitchen.sh \
  -file=/home/pentaho/projetos/pdtic/jobs/job_pdtic_extracao.kjb \
  -level=Basic > /tmp/pdtic.log 2>&1
```

Ao final, as pastas `saida` e `enviados` ficam **vazias**: os arquivos foram
enviados, movidos e limpos. Uma execução completa leva cerca de 15 segundos.

Para conferir o que chegou ao destino:

```bash
/home/pentaho/venvs/pdtic/bin/databricks fs ls dbfs:/FileStore/pdtic/bronze
```

**Nível de log:** mantenha `Basic`. Os níveis `Rowlevel` e `Debug` gravam o
conteúdo das variáveis, o que inclui credenciais.

### 2. Cadeia no Databricks

Execute o `99_run_all`, que encadeia os cinco notebooks e interrompe se algum
falhar. Ou rode um a um, na ordem: `10`, `20`, `30`, `31`, `32`.

Parâmetros padrão: `catalogo=pdtic`, `origem_dbfs=dbfs:/FileStore/pdtic/bronze`,
`extracao=auto`, `ciclos=*`, `status_id=3`.

`extracao=auto` usa o mês corrente. Para recarregar um mês específico, informe
`AAAA-MM`: apenas aquela partição é reescrita.

### 3. Carga do histórico (fora da rotina)

Só quando a área de negócio fornecer novas planilhas. Suba os CSV para
`dbfs:/FileStore/pdtic/historico` e rode o `90`, depois o `91`.

## Diagnóstico de falhas

Erros já enfrentados e como reconhecê-los.

### No Pentaho

| Sintoma | Causa | Solução |
|---|---|---|
| `Driver class 'com.mysql.jdbc.Driver' could not be found` | Conector MySQL ausente na instalação | Copiar o `.jar` para a pasta `lib` do PDI. O arquivo existe na instalação Enterprise do mesmo servidor |
| `The transformation path ... is invalid` | O `.ktr` não está no caminho esperado | Conferir o nome do arquivo em `ktrs/`; espaços no fim do nome são causa frequente |
| `Permissão negada` ao executar o script | Falta o bit de execução | `chmod +x /home/pentaho/projetos/pdtic/bat/*.sh` |
| `bad interpreter` | Arquivo enviado em modo binário, com quebra de linha do Windows | `sed -i 's/\r$//' <arquivo>` |
| `Arquivos enviados: 0` com dez erros | CLI do Databricks sem credencial para o usuário que executa | `databricks configure --token`, com token de escopo `files` |
| Arquivo de saída com espaço no nome | Espaço sobrando no campo Filename da transformação | Corrigir no Spoon e reenviar o `.ktr` |

### No Databricks

| Sintoma | Causa | Solução |
|---|---|---|
| `cannot be cast to "DATE" because it is malformed` | Datas vêm da bronze como texto, em formato não reconhecido | Usar a função `DATA()` do notebook, que recorta os 19 primeiros caracteres |
| `CANNOT_PARSE_TIMESTAMP` na carga do histórico | `to_timestamp` estrito lança exceção em vez de retornar nulo | `try_to_timestamp`, já aplicado em `normalizar_dt_consulta` |
| Pivot da silver sem nenhum valor | `custom_field_id` comparado como texto com lista de inteiros | `CAST` para inteiro, já aplicado no notebook `20` |
| `table or view cannot be found` na seção do Portfólio | Bronze ainda não carregada | Rodar o `10` antes; a seção se protege e apenas avisa |
| Alerta de volumetria divergente | Variação natural, ou extração incompleta | Comparar com os números de referência; o alerta só dispara fora da faixa de metade ao dobro |
| Alterações no `00` não surtem efeito | O `%run` carrega as funções na memória da sessão | Reexecutar a célula do `%run`, ou desanexar e reanexar o notebook |
| `Text '...' could not be parsed` ao ordenar/filtrar por `extracao` | Partições gravadas em formato de data diferente do esperado pela comparação | `extracao` é sempre `AAAA-MM` (ver `derivar_periodo`/`resolver_extracao` no notebook `00`) e ordena corretamente como texto puro — não usar `to_date`/`to_timestamp` sobre ela; se usar, o padrão é `'yyyy-MM'`, nunca `'MM-yyyy'` |
| `NameError: name 'pd' is not defined` no `91` | Comparação de mês usando pandas sem import | Como `extracao` é `AAAA-MM`, comparar direto como string (`mes < '2026-07'`); dispensa pandas |

### Verificação rápida após uma carga

```sql
SELECT extracao, COUNT(*) FROM pdtic.pdtic_gold.f_necessidades GROUP BY extracao;
SELECT COUNT(DISTINCT acao_id) FROM pdtic.pdtic_gold.f_necessidade_meta_acao;
SELECT COUNT(*) FROM pdtic.pdtic_gold.f_demandas;
```

Compare com os números de referência da seção anterior.

## Cuidados na leitura dos CSVs

- `multiLine` ligado: `journals.notes` e `custom_values.value` contêm quebras
  de linha dentro do campo (1.134 registros só em journals).
- `inferSchema=false`: tudo entra como texto e a tipagem acontece na silver,
  com regras explícitas. Por isso as comparações numéricas usam `CAST`.
- `TRY_CAST` nos campos de priorização manual: preserva valores negativos, que
  o `CAST AS UNSIGNED` do MySQL converteria em números de magnitude elevada.

## Notebooks legados

Os arquivos com sufixo `_JDBC_LEGADO` e `_federacao_LEGADO` não funcionam na
arquitetura atual — liam a origem por JDBC ou por Foreign Catalog. Ficam apenas
como referência da lógica original.

Acompanha `dicionario_custom_fields.xlsx`: catálogo dos 83 campos com formato,
preenchimento e vínculo por tipo de item.
