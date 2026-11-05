-- Event log para mineração de processos: uma linha por evento de cada leitura.
-- case_id = leitura (hash_id) | atividade = o que aconteceu | momento = quando aconteceu
-- Todos os horários chegam em UTC e são convertidos para Brasília só no select final.
with leituras as (
    select * from {{ ref('stg_leituras_sensores') }}
),

eventos as (
    -- 1. o ESP32 gerou a leitura (relógio do dispositivo)
    select
        hash_id,
        'Leitura gerada'                as atividade,
        leitura_em                      as momento,
        'ESP32'                         as recurso,
        1                               as ordem
    from leituras

    union all

    -- 2. uma regra disparou um alerta (no mesmo instante da leitura)
    select
        a.hash_id,
        'Alerta: ' || initcap(replace(a.problema, '_', ' ')),
        l.leitura_em,
        'ESP32',
        2
    from {{ ref('stg_alertas') }} a
    join leituras l
        on l.hash_id = a.hash_id

    union all

    -- 3. o subscriber recebeu a mensagem do broker MQTT
    select
        hash_id,
        'Recebida pelo subscriber',
        convert_timezone('UTC', recebido_em)::timestamp_ntz,
        'ingestao.py',
        3
    from leituras

    union all

    -- 4. a leitura foi gravada no arquivo .jsonl
    --    o horário está no nome do arquivo: leituras/2026-11-05T04-20-53-372Z.jsonl.gz
    select
        hash_id,
        'Gravada em lote',
        try_to_timestamp_ntz(
            regexp_substr(arquivo, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{3}'),
            'YYYY-MM-DD"T"HH24-MI-SS-FF3'
        ),
        'ingestao.py',
        4
    from leituras

    union all

    -- 5. o COPY INTO carregou a leitura na tabela raw do Snowflake
    select
        hash_id,
        'Carregada no Snowflake',
        carregado_em,
        'Snowflake',
        5
    from leituras
)

select
    hash_id                                                 as case_id,
    atividade,
    convert_timezone('UTC', 'America/Sao_Paulo', momento)   as momento,
    recurso,
    ordem
from eventos
-- leituras antigas podem não ter algum dos horários (ex.: arquivo com outro padrão de nome)
where momento is not null
order by case_id asc, ordem asc
