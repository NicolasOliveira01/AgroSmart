select
    md5(_doc:timestamp::string)     as hash_id,
    _doc:sensor_id::string          as sensor_id,
    _doc:talhao::string             as talhao,
    _doc:timestamp::timestamp_ntz   as leitura_em,
    _doc:temperatura::float         as temperatura,
    _doc:umidade_ar::float          as umidade_ar,
    _doc:umidade_solo::float        as umidade_solo,
    _doc:luminosidade::float        as luminosidade,
    _doc:risco_geral::string        as nivel_risco,
    _arquivo                        as arquivo,
    _linha_arquivo                  as linha_arquivo
from {{ source('AGROSMART', 'LEITURAS_SENSORES') }}
order by leitura_em desc