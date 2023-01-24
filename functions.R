# ----------------- #
# --- FUNCTIONS --- #
# ----------------- #

dados_dash_sinamge <- function(par) {
  # parâmetros --------------------------------------------------------------
  
  sym_1 <- as.symbol(par$years_fin[1])
  
  sym_2 <- as.symbol(par$years_fin[2])
  
  tags_dir <- webscrapANS::create_sqlite_tags()
  
  # requisição tabnet -------------------------------------------------------
  
  # beneficiários por modalidade
  
  modalidade <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Assistencia Medica",
    linha = "Competencia",
    years = par$years_tabnet,
    months = par$months,
    search_type = "uf",
    sqlite_dir = tags_dir
  ) |>
    dplyr::bind_rows() |>
    dplyr::filter(competencia != "TOTAL") |>
    dplyr::rename(trimestre = competencia)
  
  # beneficiários por região
  
  regiao <- webscrapANS::tabnet_request(
    coluna = "Grande Regiao",
    conteudo = "Assistencia Medica",
    linha = "Competencia",
    years = par$years_tabnet,
    months = par$months,
    search_type = "uf",
    sqlite_dir = tags_dir
  ) |>
    dplyr::bind_rows() |>
    dplyr::filter(competencia != "TOTAL") |>
    dplyr::rename(trimestre = competencia)
  
  # beneficiários por operadora
  
  associadas <- readxl::read_xlsx("inputs/lista_associadas_sinamge_17_01_23.xlsx") |>
    janitor::clean_names() |>
    dplyr::mutate(
      associada = 1,
      registro = as.character(cod_ans)
    ) |>
    dplyr::select(-cod_ans)
  
  operadora <- webscrapANS::tabnet_request(
    coluna = "Competencia",
    conteudo = "Assistencia Medica",
    linha = "Operadora",
    years = par$years_tabnet,
    months = par$months,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::map(dplyr::select, -total) |>
    purrr::reduce(
      dplyr::left_join,
      by = c("registro", "operadora")
    ) |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    )) |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::matches("[a-z]{3}_[0-9]{2}"),
        ~ .x |>
          as.numeric() |>
          tidyr::replace_na(0)
      )
    )
  
  operadora_mod <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Assistencia Medica",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)],
    months = par$month_margin,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    ))
  
  operadora_mod_last_year <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Assistencia Medica",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)] - 1,
    months = par$month_margin,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    )) |>
    dplyr::select(registro, total, associada)
  
  operadora_mod_odonto <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Excl. Odontologico",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)],
    months = par$month_margin,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    )) |>
    dplyr::select(registro, total, associada)
  
  operadora_mod_odonto_last_year <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Excl. Odontologico",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)] - 1,
    months = par$month_margin,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    )) |>
    dplyr::select(registro, total, associada)
  
  operadora_reg <- webscrapANS::tabnet_request(
    coluna = "Regiao",
    conteudo = "Assistencia Medica",
    search_type = "op",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)],
    months = par$month_margin,
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    ))
  
  # tratamento de datasets --------------------------------------------------
  
  tot_operadora <- operadora |>
    tidyr::pivot_longer(c(4:ncol(operadora) - 1),
                        names_to = "trimestre",
                        values_to = "beneficiarios"
    ) |>
    dplyr::mutate(
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "dez",
        replacement = "dec"
      ),
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "set",
        replacement = "sep"
      )
    ) |>
    dplyr::mutate(
      trimestre = zoo::as.yearqtr(lubridate::my(trimestre))
    ) |>
    dplyr::group_by(trimestre) |>
    dplyr::summarise(beneficiarios = sum(beneficiarios)) |>
    dplyr::arrange(trimestre)
  
  op_associadas <- operadora |>
    tidyr::pivot_longer(c(4:ncol(operadora) - 1),
                        names_to = "trimestre",
                        values_to = "beneficiarios"
    ) |>
    dplyr::filter(associada == 1) |>
    dplyr::mutate(
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "dez",
        replacement = "dec"
      ),
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "set",
        replacement = "sep"
      )
    ) |>
    dplyr::mutate(
      trimestre = zoo::as.yearqtr(lubridate::my(trimestre))
    ) |>
    dplyr::group_by(trimestre) |>
    dplyr::summarise(beneficiarios = sum(beneficiarios)) |>
    dplyr::arrange(trimestre)
  
  beneficiarios <- dplyr::left_join(
    tot_operadora,
    op_associadas,
    by = "trimestre"
  ) |>
    dplyr::rename(
      mercado = beneficiarios.x,
      associadas = beneficiarios.y
    ) |>
    dplyr::mutate(
      var_tri_assoc = (associadas / dplyr::lag(associadas, 4) - 1),
      var_tri_mercado = (mercado / dplyr::lag(mercado, 4) - 1)
    ) |>
    dplyr::filter(lubridate::year(trimestre) > 2016)
  
  medgrupo <- operadora_mod |>
    dplyr::select(registro, operadora, medicina_de_grupo, associada) |>
    dplyr::group_by(associada) |>
    dplyr::summarise(medicina_de_grupo = sum(as.numeric(medicina_de_grupo))) |>
    dplyr::mutate(percentual = medicina_de_grupo / sum(medicina_de_grupo))
  
  # dados financeiros gerais ------------------------------------------------
  
  financeiro <- readxl::read_xlsx("inputs/dados_financeiros.xlsx") |>
    pivot_wider(names_from = class_conta, values_from = saldo_final) |>
    dplyr::mutate_all(~replace(.,is.na(.),0)) |>
    dplyr::mutate(ano = zoo::as.yearqtr(ano)) |>
    janitor::clean_names() |>
    dplyr::mutate(
      res_op =
        receita_de_contraprestacoes_medica +
        outras_receitas_operacionais +
        contraprestacoes_de_corresponsabilidade_transferida_medica +
        outras_deducoes_medica -
        despesa_assistencial_medica -
        despesa_administrativa -
        despesa_de_comercializacao -
        outras_despesas_operacionais
    )
  
  financeiro_anual <- financeiro |>
    dplyr::group_by(ano) |>
    dplyr::summarise(
      receita_de_contraprestacoes = sum(receita_de_contraprestacoes_medica, na.rm = T),
      contraprestacoes_de_corresponsabilidade_transferida = sum(contraprestacoes_de_corresponsabilidade_transferida_medica, na.rm = T),
      despesa_assistencial = sum(despesa_assistencial_medica, na.rm = T),
      resultado_operacional = sum(res_op, na.rm = TRUE)
    ) |>
    dplyr::mutate(receita_de_contraprestacoes = receita_de_contraprestacoes + contraprestacoes_de_corresponsabilidade_transferida) |>
    dplyr::select(-contraprestacoes_de_corresponsabilidade_transferida) |>
    dplyr::mutate(
      dplyr::across(
        2:4,
        ~ round(.x / 1000000000, 1)
      )
    )
  
  financeiro_2 <- financeiro_anual |>
    dplyr::filter(ano %in% par$years_fin)
  
  # dados financeiros associadas --------------------------------------------
  
  fin_associadas <- financeiro |>
    dplyr::mutate(
      registro = as.character(registro),
      associada = dplyr::case_when(
        registro %in% associadas$registro ~ 1,
        TRUE ~ 0
      )
    ) |>
    dplyr::filter(
      ano %in% par$years_fin,
      associada == 1
    ) |>
    dplyr::group_by(ano) |>
    dplyr::summarise(
      receita_de_contraprestacoes = sum(receita_de_contraprestacoes, na.rm = T),
      despesa_assistencial = sum(despesa_assistencial, na.rm = T),
      resultado_operacional = sum(res_op, na.rm = TRUE)
    )
  
  fin_medg <- financeiro |>
    dplyr::filter(modalidade == "Medicina de Grupo") |>
    dplyr::group_by(ano) |>
    dplyr::summarise(
      receita_de_contraprestacoes = sum(receita_de_contraprestacoes_medica, na.rm = T),
      contraprestacoes_de_corresponsabilidade_transferida = sum(contraprestacoes_de_corresponsabilidade_transferida_medica, na.rm = T),
      despesa_assistencial = sum(despesa_assistencial_medica, na.rm = T),
      resultado_operacional = sum(res_op, na.rm = TRUE)
    ) |>
    dplyr::mutate(receita_de_contraprestacoes = receita_de_contraprestacoes + contraprestacoes_de_corresponsabilidade_transferida) |>
    dplyr::select(-contraprestacoes_de_corresponsabilidade_transferida) |>
    dplyr::mutate(dplyr::across(
      2:4,
      ~ round(.x / 1000000000, 1)
    ))
  
  fin_associadas_2 <- fin_associadas |>
    dplyr::mutate(dplyr::across(
      2:4,
      ~ round(.x / 1000000000, 2)
    ))
  
  # gráficos ----------------------------------------------------------------
  
  # gráfico beneficiários por ano
  
  operadora_reg_2 <- operadora_reg |>
    dplyr::select(-c(nao_identificado, total)) |>
    dplyr::filter(associada == 1) |>
    tidyr::pivot_longer(3:7,
                        names_to = "regiao",
                        values_to = "benef"
    ) |>
    dplyr::group_by(regiao) |>
    dplyr::summarize(benef = sum(as.numeric(benef))) |>
    dplyr::rename(benef_associadas = benef)
  
  # grafico de representatividade por regiao
  
  reg_total <- regiao |>
    dplyr::filter(trimestre == par$month_year[2])
  
  reg_total_2 <- reg_total |>
    dplyr::select(-c(nao_identificado, trimestre, total)) |>
    tidyr::pivot_longer(
      1:5,
      names_to = "regiao",
      values_to = "benef"
    ) |>
    dplyr::left_join(operadora_reg_2, by = "regiao") |>
    dplyr::mutate(taxa = benef_associadas / as.numeric(benef)) |>
    dplyr::mutate(code_region = 1:dplyr::n())
  
  # crescimento de beneficiários no 4º trimestre de 2021
  
  totaltri <- modalidade |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::select(total) |>
    dplyr::summarise(total = round((as.numeric(total[2]) / as.numeric(total[1]) - 1) * 100, 1)) |>
    dplyr::pull()
  
  # beneficiários que estão na modalidade Medicina de Grupo
  
  medgtri <- modalidade |>
    dplyr::filter(trimestre == par$month_year[2]) |>
    dplyr::summarise(medicina_de_grupo = 100 * round(as.numeric(medicina_de_grupo) / as.numeric(total), 3)) |>
    dplyr::pull()
  
  # faturamento das operadoras com contraprestações em 2021
  
  fat_total <- financeiro_2 |>
    dplyr::select(ano, receita_de_contraprestacoes) |>
    dplyr::filter(ano == par$years_fin[2]) |>
    dplyr::pull(receita_de_contraprestacoes)
  
  # valor gasto com despesas assistenciais
  
  desp_total <- financeiro_2 |>
    dplyr::select(ano, despesa_assistencial) |>
    dplyr::filter(ano == par$years_fin[2]) |>
    dplyr::pull(despesa_assistencial)
  
  # beneficiários por região
  
  regiao_2 <- regiao |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::select(1:6) |>
    dplyr::mutate(dplyr::across(
      2:6,
      as.numeric
    )) |>
    dplyr::rename(
      Norte = norte,
      Nordeste = nordeste,
      Sudeste = sudeste,
      Sul = sul,
      `Centro-Oeste` = centro_oeste
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "trimestre", values_from = "value") |>
    dplyr::rename(`Regiões` = name) |>
    dplyr::mutate(
      dplyr::across(
        2:3,
        ~ round(.x / 1000000, 1)
      ),
      `Regiões` = factor(
        `Regiões`,
        levels = c(
          "Norte",
          "Centro-Oeste",
          "Nordeste",
          "Sul",
          "Sudeste"
        )
      )
    ) |>
    dplyr::arrange(`Regiões`) |>
    dplyr::group_by(`Regiões`)
  
  # variação anual beneficiários por região
  
  var_reg <- regiao |>
    dplyr::select(1:6) |>
    dplyr::mutate(
      dplyr::across(
        2:6,
        as.numeric
      )
    ) |>
    dplyr::rename(
      Norte = norte,
      Nordeste = nordeste,
      Sudeste = sudeste,
      Sul = sul,
      `Centro-Oeste` = centro_oeste
    ) |>
    tidyr::separate(trimestre, c("mes", "ano")) |>
    dplyr::mutate(mes = factor(mes,
                               levels = c(
                                 "mar",
                                 "jun",
                                 "set",
                                 "dez"
                               )
    )) |>
    dplyr::arrange(ano, mes) |>
    dplyr::mutate(
      Norte = (Norte / lag(Norte, 4)) - 1,
      Nordeste = (Nordeste / lag(Nordeste, 4)) - 1,
      Sudeste = (Sudeste / lag(Sudeste, 4)) - 1,
      `Centro-Oeste` = (`Centro-Oeste` / lag(`Centro-Oeste`, 4)) - 1,
      Sul = (Sul / lag(Sul, 4)) - 1
    ) |>
    dplyr::filter(ano != 15) |>
    dplyr::mutate(mes = dplyr::case_when(
      mes == "mar" ~ "03",
      mes == "jun" ~ "06",
      mes == "set" ~ "09",
      mes == "dez" ~ "12",
    )) |>
    tidyr::unite(ano, mes, col = "trimestre", sep = "-") |>
    dplyr::mutate(trimestre = lubridate::ym(trimestre))
  
  # beneficiários por modalidade
  
  mod_1 <- modalidade |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::mutate(
      dplyr::across(
        2:7,
        ~ round(as.numeric(.x) / 1000000, 1)
      )
    ) |>
    dplyr::rename(
      Filantropia = filantropia,
      `Autogestão` = autogestao,
      Seguradoras = seguradora_especializada_em_saude,
      `Cooperativa Médica` = cooperativa_medica,
      `Medicina de Grupo` = medicina_de_grupo,
      Total = total
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "trimestre", values_from = "value") |>
    dplyr::rename(Modalidade = name)
  
  xform <- list(
    categoryorder = "array",
    categoryarray = c(
      "Filantropia",
      "Autogestão",
      "Seguradoras",
      "Cooperativa Médica",
      "Medicina de Grupo",
      "Total"
    )
  )
  
  # número de beneficiários das operadoras associadas
  
  desmp_ind <- operadora_mod |>
    dplyr::filter(associada == 1) |>
    dplyr::select(registro, operadora, total) |>
    dplyr::left_join(
      operadora_mod_last_year,
      by = "registro"
    ) |>
    dplyr::mutate(
      `VARIAÇÃO ANUAL MÉDICO` = round(100 * (as.numeric(total.x) / as.numeric(total.y) - 1), 1),
      `BENEFICIÁRIOS MÉDICOS` = as.numeric(total.x)
    ) |>
    dplyr::left_join(
      operadora_mod_odonto,
      by = "registro"
    ) |>
    dplyr::left_join(
      operadora_mod_odonto_last_year,
      by = "registro"
    ) |>
    dplyr::mutate(
      dplyr::across(
        where(is.numeric),
        tidyr::replace_na,
        0
      )
    ) |>
    dplyr::mutate(
      `VARIAÇÃO ANUAL ODONTO` = round(100 * (as.numeric(total.x.x) / as.numeric(total.y.y) - 1), 1),
      `BENEFICIÁRIOS ODONTO` = as.numeric(total.x.x)
    ) |>
    dplyr::rename(
      OPERADORA = operadora,
      REGISTRO = registro
    ) |>
    dplyr::select(REGISTRO, OPERADORA, `BENEFICIÁRIOS MÉDICOS`, `VARIAÇÃO ANUAL MÉDICO`, `BENEFICIÁRIOS ODONTO`, `VARIAÇÃO ANUAL ODONTO`) |>
    dplyr::mutate(
      `VARIAÇÃO ANUAL ODONTO` = dplyr::case_when(
        is.na(`VARIAÇÃO ANUAL ODONTO`) ~ "-",
        TRUE ~ paste0(`VARIAÇÃO ANUAL ODONTO`, "%")
      ),
      `VARIAÇÃO ANUAL MÉDICO` = dplyr::case_when(
        is.na(`VARIAÇÃO ANUAL MÉDICO`) ~ "-",
        TRUE ~ paste0(`VARIAÇÃO ANUAL MÉDICO`, "%")
      ),
      `BENEFICIÁRIOS ODONTO` = tidyr::replace_na(`BENEFICIÁRIOS ODONTO`, 0)
    )
  
  soma_med <- desmp_ind |>
    dplyr::summarise(soma_med = sum(`BENEFICIÁRIOS MÉDICOS`)) |>
    dplyr::pull()
  
  soma_odonto <- desmp_ind |>
    dplyr::summarise(soma_odonto = sum(`BENEFICIÁRIOS ODONTO`)) |>
    dplyr::pull()
  
  total <- tibble::tibble(
    REGISTRO = "TOTAL",
    OPERADORA = "",
    `BENEFICIÁRIOS MÉDICOS` = soma_med,
    `VARIAÇÃO ANUAL MÉDICO` = "",
    `BENEFICIÁRIOS ODONTO` = soma_odonto,
    `VARIAÇÃO ANUAL ODONTO` = ""
  )
  
  desmp_ind <- desmp_ind |>
    dplyr::mutate(REGISTRO = as.character(REGISTRO)) |>
    dplyr::bind_rows(total)
  
  # total de beneficiários das associadas
  
  total_benef <- desmp_ind |>
    dplyr::filter(REGISTRO != "TOTAL") |>
    dplyr::summarise(
      tot_benef = round((sum(`BENEFICIÁRIOS MÉDICOS`) + sum(`BENEFICIÁRIOS ODONTO`)) / 1000000, 1)
    ) |>
    dplyr::pull()
  
  # crescimento das associadas entre o 1º tri de 2021 e 2022
  
  var_a <- beneficiarios |>
    dplyr::filter(stringr::str_detect(trimestre, par$tri[2])) |>
    dplyr::select(var_tri_assoc) |>
    dplyr::mutate(var_tri_assoc = round(var_tri_assoc * 100, 1)) |>
    dplyr::pull()
  
  # mercado de planos médico-hospitalares coberto por uma operadora associada
  
  perc_a <- beneficiarios |>
    dplyr::filter(stringr::str_detect(trimestre, par$tri[2])) |>
    dplyr::select(mercado, associadas) |>
    dplyr::mutate(porcent = associadas / mercado) |>
    dplyr::pull(porcent)
  
  # representatividade das associadas abramge no mercado por região
  
  reg_total_3 <- reg_total_2 |>
    dplyr::mutate(
      regiao = dplyr::case_when(
        regiao == "centro_oeste" ~ "Centro-Oeste",
        regiao == "sul" ~ "Sul",
        regiao == "norte" ~ "Norte",
        regiao == "sudeste" ~ "Sudeste",
        regiao == "nordeste" ~ "Nordeste"
      ),
      regiao = factor(regiao, levels = c(
        "Nordeste",
        "Sudeste",
        "Norte",
        "Sul",
        "Centro-Oeste"
      ))
    ) |>
    dplyr::arrange(-taxa)
  
  # crescimento das receitas de contraprestações das associadas entre o 4º trimestre de 2020 e 2021
  
  fat_var <- fin_associadas_2 |>
    dplyr::rename(
      `Receitas de Contraprestações` = receita_de_contraprestacoes,
      `Despesa Assistencial` = despesa_assistencial,
      `Resultado Operacional` = resultado_operacional
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "ano", values_from = "value") |>
    dplyr::rename(tipo = name) |>
    dplyr::mutate(var = round(100 * ({{ sym_2 }} / {{ sym_1 }} - 1), 1))
  
  fat_var_1 <- fat_var |>
    dplyr::filter(tipo == "Receitas de Contraprestações") |>
    dplyr::pull(var)
  
  # aumento da despesa assistencial das associadas no período
  
  fat_var_2 <- fat_var |>
    dplyr::filter(tipo == "Despesa Assistencial") |>
    dplyr::pull(var)
  
  # variação do resultado operacional bruto das associadas no período
  
  fat_var_3 <- fat_var |>
    dplyr::filter(tipo == "Resultado Operacional") |>
    dplyr::pull(var)
  
  # associadas abramge
  
  fat_var_fac <- fat_var |>
    dplyr::mutate(tipo = factor(
      tipo,
      levels = c(
        "Receitas de Contraprestações",
        "Despesa Assistencial",
        "Resultado Operacional"
      )
    )) |>
    dplyr::arrange(tipo)
  
  # total medicinas de grupo
  
  mes_fin <- regiao |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::pull(trimestre) |>
    stringr::str_replace(
      pattern = "/",
      replacement = " "
    ) |>
    purrr::map_chr(
      stringr::str_replace,
      "2",
      "202"
    ) |>
    stringr::str_to_title()
  
  fin_medg <- fin_medg |>
    dplyr::rename(
      `Receitas de Contraprestações` = receita_de_contraprestacoes,
      `Despesa Assistencial` = despesa_assistencial,
      `Resultado Operacional` = resultado_operacional
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "ano", values_from = "value") |>
    dplyr::rename(tipo = name) |>
    dplyr::select(tipo, {{ sym_1 }}, {{ sym_2 }}) |>
    dplyr::mutate(tipo = factor(
      tipo,
      levels = c(
        "Receitas de Contraprestações",
        "Despesa Assistencial",
        "Resultado Operacional"
      )
    )) |>
    dplyr::arrange(tipo)
  
  # receitas e despesas das operadoras médico-hospitalares no 2º semestre
  
  financeiro_anual_2 <- financeiro_anual |>
    dplyr::rename(
      `Receitas de Contraprestações` = receita_de_contraprestacoes,
      `Despesa Assistencial` = despesa_assistencial,
      `Resultado Operacional` = resultado_operacional
    )
  
  return(
    list(
      totaltri,
      medgtri,
      financeiro_2,
      fat_total,
      desp_total,
      regiao_2,
      var_reg,
      mod_1,
      associadas,
      total_benef,
      var_a,
      perc_a,
      reg_total_3,
      medgrupo,
      beneficiarios,
      fat_var_1,
      fat_var_2,
      fat_var_3,
      fat_var_fac,
      fin_medg,
      financeiro_anual_2,
      desmp_ind,
      xform,
      mes_fin
    )
  )
}

dados_dash_sinog <- function(par) {
  # parâmetros --------------------------------------------------------------
  
  sym_1 <- as.symbol(par$years_fin[1])
  
  sym_2 <- as.symbol(par$years_fin[2])
  
  tags_dir <- webscrapANS::create_sqlite_tags()
  
  # requisição tabnet -------------------------------------------------------
  
  # beneficiários por modalidade
  
  modalidade <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Excl. Odontologico",
    linha = "Competencia",
    years = par$years_tabnet,
    months = par$months,
    search_type = "uf",
    sqlite_dir = tags_dir
  ) |>
    dplyr::bind_rows() |>
    dplyr::filter(competencia != "TOTAL") |>
    dplyr::rename(trimestre = competencia)
  
  # beneficiários por região
  
  regiao <- webscrapANS::tabnet_request(
    coluna = "Grande Regiao",
    conteudo = "Excl. Odontologico",
    linha = "Competencia",
    years = par$years_tabnet,
    months = par$months,
    search_type = "uf",
    sqlite_dir = tags_dir
  ) |>
    dplyr::bind_rows() |>
    dplyr::filter(competencia != "TOTAL") |>
    dplyr::rename(trimestre = competencia)
  
  # beneficiários por operadora
  
  associadas <- readxl::read_xlsx("inputs/lista_associadas_sinog_04_05_22.xlsx") |>
    janitor::clean_names() |>
    dplyr::mutate(
      associada = 1,
      registro = as.character(numero_ans)
    ) |>
    dplyr::select(-numero_ans)
  
  operadora <- webscrapANS::tabnet_request(
    coluna = "Competencia",
    conteudo = "Excl. Odontologico",
    linha = "Operadora",
    years = par$years_tabnet,
    months = par$months,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::map(dplyr::select, -total) |>
    purrr::reduce(
      dplyr::left_join,
      by = c("registro", "operadora")
    ) |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    )) |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::matches("[a-z]{3}_[0-9]{2}"),
        ~ .x |>
          as.numeric() |>
          tidyr::replace_na(0)
      )
    )
  
  operadora_mod <- webscrapANS::tabnet_request(
    coluna = "Modalidade",
    conteudo = "Excl. Odontologico",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)],
    months = par$month_margin,
    search_type = "op",
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    ))
  
  operadora_reg <- webscrapANS::tabnet_request(
    coluna = "Regiao",
    conteudo = "Excl. Odontologico",
    search_type = "op",
    linha = "Operadora",
    years = par$years_tabnet[length(par$years_tabnet)],
    months = par$month_margin,
    sqlite_dir = tags_dir
  ) |>
    purrr::flatten_dfr() |>
    dplyr::filter(operadora != "TOTAL") |>
    dplyr::mutate(associada = dplyr::case_when(
      registro %in% associadas$registro ~ 1,
      TRUE ~ 0
    ))
  
  # tratamento de datasets --------------------------------------------------
  
  tot_operadora <- operadora |>
    tidyr::pivot_longer(c(4:ncol(operadora) - 1),
                        names_to = "trimestre",
                        values_to = "beneficiarios"
    ) |>
    dplyr::mutate(
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "dez",
        replacement = "dec"
      ),
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "set",
        replacement = "sep"
      )
    ) |>
    dplyr::mutate(
      trimestre = zoo::as.yearqtr(lubridate::my(trimestre))
    ) |>
    dplyr::group_by(trimestre) |>
    dplyr::summarise(beneficiarios = sum(beneficiarios)) |>
    dplyr::arrange(trimestre)
  
  op_associadas <- operadora |>
    tidyr::pivot_longer(c(4:ncol(operadora) - 1),
                        names_to = "trimestre",
                        values_to = "beneficiarios"
    ) |>
    dplyr::filter(associada == 1) |>
    dplyr::mutate(
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "dez",
        replacement = "dec"
      ),
      trimestre = stringr::str_replace_all(
        string = trimestre,
        pattern = "set",
        replacement = "sep"
      )
    ) |>
    dplyr::mutate(
      trimestre = zoo::as.yearqtr(lubridate::my(trimestre))
    ) |>
    dplyr::group_by(trimestre) |>
    dplyr::summarise(beneficiarios = sum(beneficiarios)) |>
    dplyr::arrange(trimestre)
  
  beneficiarios <- dplyr::left_join(
    tot_operadora,
    op_associadas,
    by = "trimestre"
  ) |>
    dplyr::rename(
      mercado = beneficiarios.x,
      associadas = beneficiarios.y
    ) |>
    dplyr::mutate(
      var_tri_assoc = (associadas / dplyr::lag(associadas, 4) - 1),
      var_tri_mercado = (mercado / dplyr::lag(mercado, 4) - 1)
    ) |>
    dplyr::filter(lubridate::year(trimestre) > 2016)
  
  odontogrupo <- operadora_mod |>
    dplyr::select(registro, operadora, odontologia_de_grupo, total, associada) |>
    dplyr::group_by(associada) |>
    dplyr::summarise(odontologia_de_grupo = sum(as.numeric(odontologia_de_grupo))) |>
    dplyr::mutate(percentual = odontologia_de_grupo / sum(odontologia_de_grupo))
  
  # dados financeiros gerais ------------------------------------------------
  
  financeiro <- readxl::read_xlsx("inputs/dados_financeiros.xlsx") |>
    dplyr::mutate(ano = lubridate::year(zoo::as.yearqtr(ano_tri))) |>
    dplyr::select(-ano_tri) |>
    janitor::clean_names() |>
    dplyr::mutate(
      res_op =
        receita_de_contraprestacoes_odonto +
        contraprestacoes_de_corresponsabilidade_transferida_odonto +
        outras_deducoes_odonto -
        despesa_assistencial_odonto
    )
  
  financeiro_anual <- financeiro |>
    dplyr::group_by(ano) |>
    dplyr::summarise(
      receita_de_contraprestacoes_odonto = sum(receita_de_contraprestacoes_odonto, na.rm = T),
      contraprestacoes_de_corresponsabilidade_transferida_odonto = sum(contraprestacoes_de_corresponsabilidade_transferida_odonto, na.rm = T),
      despesa_assistencial_odonto = sum(despesa_assistencial_odonto, na.rm = T),
      resultado_operacional = sum(res_op, na.rm = TRUE)
    ) |>
    dplyr::mutate(receita_de_contraprestacoes_odonto = receita_de_contraprestacoes_odonto + contraprestacoes_de_corresponsabilidade_transferida_odonto) |>
    dplyr::select(-contraprestacoes_de_corresponsabilidade_transferida_odonto) |>
    dplyr::mutate(
      dplyr::across(
        2:4,
        ~ round(.x / 1000000000, 1)
      )
    )
  
  financeiro_2 <- financeiro_anual |>
    dplyr::filter(ano %in% par$years_fin)
  
  # dados financeiros associadas --------------------------------------------
  
  fin_associadas <- financeiro |>
    dplyr::mutate(
      registro = as.character(registro),
      associada = dplyr::case_when(
        registro %in% associadas$registro ~ 1,
        TRUE ~ 0
      )
    ) |>
    dplyr::filter(
      ano %in% par$years_fin,
      associada == 1
    ) |>
    dplyr::group_by(ano) |>
    dplyr::summarise(
      receita_de_contraprestacoes_odonto = sum(receita_de_contraprestacoes_odonto, na.rm = T),
      despesa_assistencial_odonto = sum(despesa_assistencial_odonto, na.rm = T),
      resultado_operacional = sum(res_op, na.rm = TRUE)
    )
  
  fin_associadas_2 <- fin_associadas |>
    dplyr::mutate(dplyr::across(
      2:4,
      ~ round(.x / 1000000000, 2)
    ))
  
  # gráficos ----------------------------------------------------------------
  
  # gráfico beneficiários por ano
  
  operadora_reg_2 <- operadora_reg |>
    dplyr::select(-c(nao_identificado, total)) |>
    dplyr::filter(associada == 1) |>
    tidyr::pivot_longer(3:7,
                        names_to = "regiao",
                        values_to = "benef"
    ) |>
    dplyr::group_by(regiao) |>
    dplyr::summarize(benef = sum(as.numeric(benef))) |>
    dplyr::rename(benef_associadas = benef)
  
  # grafico de representatividade por regiao
  
  reg_total <- regiao |>
    dplyr::filter(trimestre == par$month_year[2])
  
  reg_total_2 <- reg_total |>
    dplyr::select(-c(nao_identificado, trimestre, total)) |>
    tidyr::pivot_longer(
      1:5,
      names_to = "regiao",
      values_to = "benef"
    ) |>
    dplyr::left_join(operadora_reg_2, by = "regiao") |>
    dplyr::mutate(taxa = benef_associadas / as.numeric(benef)) |>
    dplyr::mutate(code_region = 1:n())
  
  # crescimento de beneficiários no 4º trimestre de 2021
  
  totaltri <- modalidade |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::select(total) |>
    dplyr::summarise(total = round((as.numeric(total[2]) / as.numeric(total[1]) - 1) * 100, 1)) |>
    dplyr::pull()
  
  # beneficiários que estão na modalidade Medicina de Grupo
  
  odontogtri <- modalidade |>
    dplyr::filter(trimestre == par$month_year[2]) |>
    dplyr::summarise(odontologia_de_grupo = 100 * round(as.numeric(odontologia_de_grupo) / as.numeric(total), 3)) |>
    dplyr::pull()
  
  # faturamento das operadoras com contraprestações em 2021
  
  fat_total <- financeiro_2 |>
    dplyr::select(ano, receita_de_contraprestacoes_odonto) |>
    dplyr::filter(ano == par$years_fin[2]) |>
    dplyr::pull(receita_de_contraprestacoes_odonto)
  
  # valor gasto com despesas assistenciais
  
  desp_total <- financeiro_2 |>
    dplyr::select(ano, despesa_assistencial_odonto) |>
    dplyr::filter(ano == par$years_fin[2]) |>
    dplyr::pull(despesa_assistencial_odonto)
  
  # beneficiários por região
  
  regiao_2 <- regiao |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::select(1:6) |>
    dplyr::mutate(dplyr::across(
      2:6,
      as.numeric
    )) |>
    dplyr::rename(
      Norte = norte,
      Nordeste = nordeste,
      Sudeste = sudeste,
      Sul = sul,
      `Centro-Oeste` = centro_oeste
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "trimestre", values_from = "value") |>
    dplyr::rename(`Regiões` = name) |>
    dplyr::mutate(
      dplyr::across(
        2:3,
        ~ round(.x / 1000000, 1)
      ),
      `Regiões` = factor(
        `Regiões`,
        levels = c(
          "Norte",
          "Centro-Oeste",
          "Sul",
          "Nordeste",
          "Sudeste"
        )
      )
    ) |>
    dplyr::arrange(`Regiões`) |>
    dplyr::group_by(`Regiões`)
  
  # variação anual beneficiários por região
  
  var_reg <- regiao |>
    dplyr::select(1:6) |>
    dplyr::mutate(
      dplyr::across(
        2:6,
        as.numeric
      )
    ) |>
    dplyr::rename(
      Norte = norte,
      Nordeste = nordeste,
      Sudeste = sudeste,
      Sul = sul,
      `Centro-Oeste` = centro_oeste
    ) |>
    tidyr::separate(trimestre, c("mes", "ano")) |>
    dplyr::mutate(mes = factor(mes,
                               levels = c(
                                 "mar",
                                 "jun",
                                 "set",
                                 "dez"
                               )
    )) |>
    dplyr::arrange(ano, mes) |>
    dplyr::mutate(
      Norte = (Norte / lag(Norte, 4)) - 1,
      Nordeste = (Nordeste / lag(Nordeste, 4)) - 1,
      Sudeste = (Sudeste / lag(Sudeste, 4)) - 1,
      `Centro-Oeste` = (`Centro-Oeste` / lag(`Centro-Oeste`, 4)) - 1,
      Sul = (Sul / lag(Sul, 4)) - 1
    ) |>
    dplyr::filter(ano != 15) |>
    dplyr::mutate(mes = dplyr::case_when(
      mes == "mar" ~ "03",
      mes == "jun" ~ "06",
      mes == "set" ~ "09",
      mes == "dez" ~ "12",
    )) |>
    tidyr::unite(ano, mes, col = "trimestre", sep = "-") |>
    dplyr::mutate(trimestre = lubridate::ym(trimestre))
  
  # beneficiários por modalidade
  
  mod_1 <- modalidade |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::mutate(
      dplyr::across(
        2:9,
        ~ round(as.numeric(.x) / 1000000, 1)
      )
    ) |>
    dplyr::rename(
      Filantropia = filantropia,
      `Autogestão` = autogestao,
      Seguradoras = seguradora_especializada_em_saude,
      `Cooperativa Odontológica` = cooperativa_odontologica,
      `Odontologia de Grupo` = odontologia_de_grupo,
      `Medicina de Grupo` = medicina_de_grupo,
      `Cooperativa Médica` = cooperativa_medica,
      Total = total
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "trimestre", values_from = "value") |>
    dplyr::rename(Modalidade = name)
  
  xform <- list(
    categoryorder = "array",
    categoryarray = c(
      "Autogestão",
      "Filantropia",
      "Cooperativa Médica",
      "Seguradoras",
      "Cooperativa Odontológica",
      "Medicina de Grupo",
      "Odontologia de Grupo",
      "Total"
    )
  )
  
  # total de beneficiários das associadas
  
  total_benef <- beneficiarios |>
    dplyr::filter(trimestre == par$tri[2]) |>
    dplyr::summarise(
      tot_benef = round(associadas / 1000000, 1)
    ) |>
    dplyr::pull()
  
  # crescimento das associadas entre o 1º tri de 2021 e 2022
  
  var_a <- beneficiarios |>
    dplyr::filter(stringr::str_detect(trimestre, par$tri[2])) |>
    dplyr::select(var_tri_assoc) |>
    dplyr::mutate(var_tri_assoc = round(var_tri_assoc * 100, 1)) |>
    dplyr::pull()
  
  # mercado de planos exclusivamente odontológicos coberto por uma operadora associada
  
  exclu_odonto <- operadora_mod |>
    dplyr::select(registro, total, associada) |>
    dplyr::group_by(associada) |>
    dplyr::summarise(exclusivo_odonto = sum(as.numeric(total))) |>
    dplyr::mutate(percentual = exclusivo_odonto / sum(exclusivo_odonto))
  
  # representatividade das associadas abramge no mercado por região
  
  reg_total_3 <- reg_total_2 |>
    dplyr::mutate(
      regiao = dplyr::case_when(
        regiao == "centro_oeste" ~ "Centro-Oeste",
        regiao == "sul" ~ "Sul",
        regiao == "norte" ~ "Norte",
        regiao == "sudeste" ~ "Sudeste",
        regiao == "nordeste" ~ "Nordeste"
      ),
      regiao = factor(regiao, levels = c(
        "Nordeste",
        "Sudeste",
        "Norte",
        "Sul",
        "Centro-Oeste"
      ))
    ) |>
    dplyr::arrange(-taxa)
  
  # crescimento das receitas de contraprestações das associadas entre o 4º trimestre de 2020 e 2021
  
  fat_var <- fin_associadas_2 |>
    dplyr::rename(
      `Receitas de Contraprestações Odonto` = receita_de_contraprestacoes_odonto,
      `Despesa Assistencial Odonto` = despesa_assistencial_odonto,
      `Resultado Operacional` = resultado_operacional
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "ano", values_from = "value") |>
    dplyr::rename(tipo = name) |>
    dplyr::mutate(var = round(100 * ({{ sym_2 }} / {{ sym_1 }} - 1), 1))
  
  fat_var_1 <- fat_var |>
    dplyr::filter(tipo == "Receitas de Contraprestações Odonto") |>
    dplyr::pull(var)
  
  # aumento da despesa assistencial das associadas no período
  
  fat_var_2 <- fat_var |>
    dplyr::filter(tipo == "Despesa Assistencial Odonto") |>
    dplyr::pull(var)
  
  # variação do resultado operacional bruto das associadas no período
  
  fat_var_3 <- fat_var |>
    dplyr::filter(tipo == "Resultado Operacional") |>
    dplyr::pull(var)
  
  # associadas sinog
  
  fat_var_fac <- fat_var |>
    dplyr::mutate(tipo = factor(
      tipo,
      levels = c(
        "Receitas de Contraprestações Odonto",
        "Despesa Assistencial Odonto",
        "Resultado Operacional"
      )
    )) |>
    dplyr::arrange(tipo)
  
  # total planos odontológicos
  
  mes_fin <- regiao |>
    dplyr::arrange(trimestre) |>
    dplyr::filter(stringr::str_detect(trimestre, par$month_year[1])) |>
    dplyr::pull(trimestre) |>
    stringr::str_replace(
      pattern = "/",
      replacement = " "
    ) |>
    purrr::map_chr(
      stringr::str_replace,
      "2",
      "202"
    ) |>
    stringr::str_to_title()
  
  fin_tot_odonto <- financeiro_anual |>
    dplyr::rename(
      `Receitas de Contraprestações Odonto` = receita_de_contraprestacoes_odonto,
      `Despesa Assistencial Odonto` = despesa_assistencial_odonto,
      `Resultado Operacional` = resultado_operacional
    ) |>
    tidyr::pivot_longer(cols = -1) |>
    tidyr::pivot_wider(names_from = "ano", values_from = "value") |>
    dplyr::rename(tipo = name) |>
    dplyr::select(tipo, {{ sym_1 }}, {{ sym_2 }}) |>
    dplyr::mutate(tipo = factor(
      tipo,
      levels = c(
        "Receitas de Contraprestações Odonto",
        "Despesa Assistencial Odonto",
        "Resultado Operacional"
      )
    )) |>
    dplyr::arrange(tipo)
  
  # receitas e despesas do segmento excl. odontológico no 2º semestre
  
  financeiro_anual_2 <- financeiro_anual |>
    dplyr::rename(
      `Receitas de Contraprestações Odonto` = receita_de_contraprestacoes_odonto,
      `Despesa Assistencial Odonto` = despesa_assistencial_odonto,
      `Resultado Operacional` = resultado_operacional
    )
  
  return(
    list(
      totaltri,
      odontogtri,
      financeiro_2,
      fat_total,
      desp_total,
      regiao_2,
      var_reg,
      mod_1,
      associadas,
      total_benef,
      var_a,
      reg_total_3,
      exclu_odonto,
      beneficiarios,
      fat_var_1,
      fat_var_2,
      fat_var_3,
      fat_var_fac,
      fin_tot_odonto,
      financeiro_anual_2,
      xform,
      mes_fin
    )
  )
}
