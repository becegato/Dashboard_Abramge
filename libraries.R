# ----------------- #
# --- LIBRARIES --- #
# ----------------- #

if (!require("pacman")) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(
  rvest, 
  httr, 
  usethis,
  janitor, 
  tidyverse, 
  RSQLite, 
  zoo, 
  lubridate, 
  ggthemes, 
  colourpicker, 
  flexdashboard, 
  knitr, 
  plotly, 
  kableExtra, 
  ggtext, 
  ggExtra, 
  geobr, 
  styler, 
  lintr,
  zeallot
)

pacman::p_load_gh("becegato/webscrapANS")
