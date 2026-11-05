select
    hash_id,
    sensor_id,
    talhao,
    convert_timezone('UTC', 'America/Sao_Paulo', leitura_em) as leitura_em,
    temperatura,
    umidade_ar,
    umidade_solo,
    luminosidade,
    nivel_risco as risco_geral,
    case nivel_risco
        when 'Nenhum' then 0
        when 'Baixo'  then 1
        when 'Médio'  then 2
        when 'Alto'   then 3
    end as nivel_risco
from {{ ref('stg_leituras_sensores') }}