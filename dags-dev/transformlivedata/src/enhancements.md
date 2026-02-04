Ideia testada para  gerar as viagens gradualmente porém sem sucesso

pipeline:
transforma dados brutos em um dataframe em memoria, enriquecendo com trip_details:
- salva full em trusted.positions
- salva só campos relevantes em trusted.latest_positions 

criar uma tabela de trusted.ongoing_trips com chave composta trip_id e veiculo_id
começa vazia

carrega trusted.latest_positions 
carrega trusted.ongoing_trips
para cada posicao:
- se tem ongoing_trip:
    carrega ultima posicao da ongoing_trip
    se posicao_atual.sentido <> ultima_posicao.sentido:
        end_time = ts da posicao anterior
        se qualificada salva em finished_trips
        senao salva em broken_trips
        atualiza ongoing_trip com o dado da posicao atual
    se nao:
        atualiza posicao e veiculo_ts
  se nao:
    cria trip com start_time = ts da posicao atual
