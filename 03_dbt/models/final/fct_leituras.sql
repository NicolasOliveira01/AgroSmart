select
    hash_id,
    sensor_id,
    talhao,
    convert_timezone('UTC', 'America/Sao_Paulo', leitura_em) as leitura_em,
    temperatura,
    umidade_ar,
    umidade_solo,
    luminosidade,
    case nivel_risco
        when 0 then 'Nenhum'
        when 1 then 'Baixo'
        when 2 then 'Médio'
        when 3 then 'Alto'
    end as risco_geral,
    nivel_risco
from {{ ref('stg_leituras_sensores') }}