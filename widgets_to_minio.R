# BOILERPLATE =================================================================================================
rm(list=ls())
options(scipen=999)
args = commandArgs(trailingOnly=TRUE)

# SETENV FOR DAG COMPATIBILITY ===================================================================================
if (Sys.getenv("DB_UTILS_DIR") == "") {
  Sys.setenv("DB_UTILS_DIR" = "~/db-utils")
}
if (Sys.getenv("SECRETS_FILE") == "") {
  Sys.setenv("SECRETS_FILE" = "~/secrets.json")
}

# LOAD LIBRARIES ============================================================================
source(file.path(Sys.getenv("DB_UTILS_DIR"), "/R/db-utils.R"), chdir = TRUE)
library(tidyverse)
library(flexdashboard)
library(readr)           
library(lubridate)
library(xts)
library(dygraphs)
library(RColorBrewer)
library(jsonlite)
library(DT)
library(plotly)
library(htmlwidgets)
library(tools)
library(leaflet)
library(arrow)
library(sf)
library(leafpop)
library(bpexploder)
library(sparkline)

# LOAD SECRETS ==========================================================================
# Credentials
secrets <- fromJSON(Sys.getenv("SECRETS_FILE"))
# Load minio credentials
minio_key <- secrets$minio$edge$access
minio_secret <- secrets$minio$edge$secret
data_classification = "EDGE"
filename_prefix_override = NA
minio_url_override = NA


# FUNCTIONS =================================================================
df_as_xts <- function(df, time_col) {
  df2 <- df %>% 
    select(-time_col)
  
  df3 <- df2 %>% 
    dplyr::select(names(sort(colSums(df2, na.rm = T)))) 
  
  xdf <- xts(df3, order.by= df %>% pull(time_col))
  xdf
}

# Add values to list
listN <- function(...){
  anonList <- list(...)
  names(anonList) <- as.character(substitute(list(...)))[-1]
  anonList
}

save_widget <- function(widg, destdir) {
  savepath <- file.path(getwd(), destdir, 
                        paste(deparse(substitute(widg)), "html", sep = "."))
  libdir <- file.path(getwd(), destdir, 
                      "libdir")
  if (!(file.exists(libdir))) {
    dir.create(libdir)
  }
  if (!("htmlwidget" %in% class(widg))) {
    stop("Not an htmlwidget!")
  } else {
    widg$sizingPolicy$padding = 0
    widg$sizingPolicy$browser$padding = 0
    widg$sizingPolicy$viewer$padding = 0
    saveWidget(widg, savepath, selfcontained = F, libdir = libdir)
    print(paste("Saved to", savepath))
  }
}
# CREATE DIRS =================================================================
public_sourcedir <- "data/public"
unlink(public_sourcedir, recursive = T )
dir.create(public_sourcedir, recursive = TRUE)

private_sourcedir <- "data/private"
unlink(private_sourcedir, recursive = T )
dir.create(private_sourcedir, recursive = TRUE)

public_destdir <- "widgets/public"
unlink(public_destdir, recursive = T )
dir.create(public_destdir, recursive = TRUE)

private_destdir <- "widgets/private"
unlink(private_destdir, recursive = T )
dir.create(private_destdir, recursive = TRUE)

# PULL IN PUBLIC DATA =======================================================
covid_assets <- bucket_objects_to_df("covid", 
                     minio_key,
                     minio_secret,
                     "ds2.capetown.gov.za")


# pull in public dataset ---------------
datasets <- covid_assets %>% 
  filter(grepl("data/public",object_name) ) %>% 
  filter(grepl(".csv",object_name) | grepl(".geojson",object_name)) %>%
  pull(object_name) %>% as.character() 

for (object_name in datasets) {
  minio_to_file(object_name,
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                minio_filename_override = object_name)
}

dataset_names <- strsplit(datasets, "\\/")
dataset_names <- sapply(dataset_names, "[[", 3)
dataset_names <- strsplit(dataset_names, "\\.")
dataset_names <- sapply(dataset_names, "[[", 1) 

# Load all
for (i in seq_along(1:length(datasets))) {
  if (strsplit(datasets[i], "\\.")[[1]][2] == "csv") {
    df <- read_csv(datasets[i])
    assign(paste(dataset_names[i]), df)
    rm(df)
  } else if (strsplit(datasets[i], "\\.")[[1]][2] == "geojson") {
    df_spatial <- st_read(datasets[i])
    assign(paste(dataset_names[i]), df_spatial)
    rm(df_spatial)
  }
}
rm(datasets)
rm(dataset_names)

# pull in private dataset ---------------
datasets <- covid_assets %>% 
  filter(grepl("data/private",object_name) ) %>% 
  filter(grepl(".csv",object_name) | grepl(".geojson",object_name)) %>%
  pull(object_name) %>% as.character() 

for (object_name in datasets) {
  minio_to_file(object_name,
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                minio_filename_override = object_name)
}

dataset_names <- strsplit(datasets, "\\/")
dataset_names <- sapply(dataset_names, "[[", 3)
dataset_names <- strsplit(dataset_names, "\\.")
dataset_names <- sapply(dataset_names, "[[", 1) 

# Load all
for (i in seq_along(1:length(datasets))) {
  if (strsplit(datasets[i], "\\.")[[1]][2] == "csv") {
    df <- read_csv(datasets[i])
    assign(paste(dataset_names[i]), df)
    rm(df)
  } else if (strsplit(datasets[i], "\\.")[[1]][2] == "geojson") {
    df_spatial <- st_read(datasets[i])
    assign(paste(dataset_names[i]), df_spatial)
    rm(df_spatial)
  }
}
rm(datasets)
rm(dataset_names)

# PREPARE DATA ==========================================================
# RSA confirmed splitby source
rsa_confirmed_by_type <- covid19za_timeline_confirmed %>% 
  group_by(YYYYMMDD, type) %>% 
  summarise(confirmed = n()) %>% 
  ungroup() %>%
  spread(key = type, value = confirmed) %>%
  replace(is.na(.), 0) %>%
  mutate(local = cumsum(local),
         pending = cumsum(pending),
         travel = cumsum(travel)) 

# RSA total timeseries
sa_ts_confirmed <- rsa_provincial_ts_confirmed %>% 
  select(-YYYYMMDD, -total, -source) %>% 
  rowSums() %>% 
  enframe(value = "confirmed")

sa_ts_confirmed$YYYYMMDD <- rsa_provincial_ts_confirmed$YYYYMMDD

sa_ts_confirmed <- sa_ts_confirmed %>% select(YYYYMMDD, confirmed)

# Latest Values Global -----------------------
global_last_updated <- max(c(global_ts_sorted_confirmed$report_date,
                             global_ts_sorted_deaths$report_date))

global_last_confirmed_val <- sum(global_latest_data$confirmed)

global_last_deaths_val <- sum(global_latest_data$deaths)

# Latest Values RSA --------------------------
rsa_latest_update <-  max(rsa_provincial_ts_confirmed$YYYYMMDD)
rsa_latest_confirmed <- rsa_provincial_ts_confirmed %>% filter(YYYYMMDD == max(YYYYMMDD)) %>%  pull(total) %>% .[1]
rsa_latest_deaths <- nrow(covid19za_timeline_deaths)
rsa_latest_tested <- covid19za_timeline_testing %>% summarise(val = max(cumulative_tests, na.rm = T)) %>% pull(val) %>% .[1]

# Latest values WC ---------------------------------
wc_latest_update <- max(wc_all_cases$date_of_diagnosis1)
wc_latest_confirmed <- nrow(wc_all_cases)

# Latest values CT
ct_latest_update <- wc_latest_update
ct_latest_confirmed <- nrow(ct_all_cases)
ct_latest_deaths <- ct_all_cases %>% filter(!is.na(date_of_death)) %>% nrow() 

# expected_future_trajectory -----------------------
countries_this_far <- global_ts_since_100 %>% 
  select(-days_since_passed_100) %>% 
  apply(., MARGIN = 1, function(x) sum(!is.na(x)))

median_values <- global_ts_since_100 %>% 
  select(-days_since_passed_100) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~ median(., na.rm = T))) %>%
  t() 

lower_quartile_values <- global_ts_since_100 %>% select(-days_since_passed_100) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~quantile(., probs = 0.25, na.rm = T))) %>%
  t() 

upper_quartile_values <- global_ts_since_100 %>% select(-days_since_passed_100) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~quantile(., probs = 0.75, na.rm = T))) %>%
  t() 

values_to_drop <- ifelse(countries_this_far < 14, NA, 1)

median_values <- median_values * values_to_drop


# Age brackets -----
age_brackets <- c(0, 10, 20, 30,40,50,60,70, 80, Inf)
age_bracket_labels <- c("0 < 9", 
                        "10 < 19", 
                        "20 < 29", 
                        "30 < 39",
                        "40 < 49",
                        "50 < 59",
                        "60 < 69",
                        "70 < 79", 
                        "80 +")

# fatality by age -----------------
# China https://www.ncbi.nlm.nih.gov/pubmed/32064853?fbclid=IwAR3JCxH50VTfg3Q_02YTLdz2Tk7yBTmt-5oCxE4KlBe0evh7ByK3HPVU-pU
# https://ourworldindata.org/uploads/2020/03/Coronavirus-CFR-by-age-in-China-1.png
chinese_age_fatality_rate <- c(0, 0.2, 0.2, 0.2, 0.4, 1.3, 3.6, 8, 14.8)
sum(chinese_age_fatality_rate)

# China demographic --------
china_demographic <- global_pop_raw %>% filter(NAME == "China") 

china_demographic <- china_demographic %>% 
  mutate(age_interval = findInterval(china_demographic$AGE, age_brackets, rightmost.closed = TRUE)) 

china_demographic<- china_demographic %>% 
  mutate(age_interval = age_bracket_labels[age_interval]) %>% 
  group_by(age_interval) %>% 
  summarise(cn_population = sum(POP)) %>% 
  ungroup() %>% 
  mutate(population_pct = cn_population/sum(cn_population) * 100) %>%
  select(-cn_population) %>%
  mutate(rate_pct = chinese_age_fatality_rate) %>%
  mutate(population = "China Pop %",
         fatal_label = "China Case Fatality Rate %") 

# RSA demographic -----------
rsa_raw_age_fatalities <- covid19za_timeline_deaths  %>%
  mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>%
  mutate(age_interval = age_bracket_labels[age_interval]) %>%
  group_by(age_interval) %>%
  summarise(rsa_raw_age_fatalities = n()) %>%
  ungroup() 

# rsa_raw_age_fatality_rate <- left_join(enframe(age_bracket_labels), 
#                                    rsa_raw_age_fatality_rate, by = c("value" = "age_interval")) %>%
#   mutate(rsa_raw_age_fatality_rate = replace_na(rsa_raw_age_fatality_rate, 0)) %>%
#   mutate(rsa_raw_age_fatality_rate = rsa_raw_age_fatality_rate/sum(rsa_raw_age_fatality_rate)) %>%
#   pull(rsa_raw_age_fatality_rate)*100

# rsa_raw_age_confirmed_cases <- covid19za_timeline_confirmed %>%
#   mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>%
#   mutate(age_interval = age_bracket_labels[age_interval]) %>%
#   group_by(age_interval) %>%
#   summarise(rsa_raw_age_confirmed_cases = n()) %>%
#   ungroup() 
# 
# rsa_age_fatality_rate <- left_join(rsa_raw_age_confirmed_cases, rsa_raw_age_fatalities, by = "age_interval") %>%
#   mutate(rsa_raw_age_fatalities = replace_na(rsa_raw_age_fatalities, 0)) %>% 
#   mutate(rsa_age_fatality_rate = rsa_raw_age_fatalities / rsa_raw_age_confirmed)
rsa_age_fatality_rate <- c(0, 0, 0, 0, 0, 0, 0, 0, 0)

rsa_demographic <- rsa_pop_genders_ages %>% 
  mutate(pop = male + female,
         age_interval = findInterval(rsa_pop_genders_ages$AGE, age_brackets, rightmost.closed = TRUE)) 

rsa_demographic <- rsa_demographic %>% 
  mutate(age_interval = age_bracket_labels[age_interval]) %>% 
  group_by(age_interval) %>% 
  summarise(rsa_population = sum(pop)) %>% 
  ungroup() %>% 
  mutate(population_pct = rsa_population/sum(rsa_population)*100) %>%
  select(-rsa_population) %>%
  mutate(rate_pct = rsa_age_fatality_rate) %>% 
  mutate(population = "SA Pop %",
         fatal_label = "SA Fatality Rate %") 

# cape town confirmed cases pop pyramid ---------------
ct_raw_age_confirmed_cases <- ct_all_cases  %>%
    select(agegroup) %>%
    separate(agegroup, sep = "[ ]", into = c("age"), extra = "drop") %>%
    mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>%
  mutate(age_interval = age_bracket_labels[age_interval]) %>%
  group_by(age_interval) %>%
  summarise(ct_raw_age_confirmed_cases = n()) %>%
  ungroup()
# 
# ct_age_confirmed_case_pct <- ct_raw_age_confirmed_cases %>% 
#   mutate(ct_age_confirmed_case_pct = ct_raw_age_confirmed_cases / sum(ct_raw_age_confirmed_cases)*100) %>% 
#   select(age_interval, ct_age_confirmed_case_pct)
# 
# cct_demographic <- cct_mid_year_2019_pop_est %>% 
#   mutate(age_interval = NORM_AGE_COHORT) %>%
#   group_by(age_interval) %>% 
#   summarise(cct_population = sum(AMOUNT)) %>% 
#   ungroup() %>%
#   mutate(population_pct = cct_population/sum(cct_population)*100) %>%
#   select(-cct_population) %>%
#   left_join(., ct_age_confirmed_case_pct, by = "age_interval") %>%
#   rename(rate_pct = ct_age_confirmed_case_pct) %>%
#   mutate(population = "CCT Pop %",
#          fatal_label = "CCT Rate % of Confirmed Cases") 

# cape town case fatality pop pyramid ---------------
ct_raw_age_deaths <- ct_all_cases  %>%
  filter(!is.na(date_of_death)) %>%
  select(agegroup) %>% 
  separate(agegroup, sep = "[ ]", into = c("age"), extra = "drop") %>%
  mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>% 
  mutate(age_interval = age_bracket_labels[age_interval]) %>%
  group_by(age_interval) %>%
  summarise(ct_raw_age_deaths = n()) %>%
  ungroup() 


ct_age_fatality_rate <- left_join(ct_raw_age_confirmed_cases, ct_raw_age_deaths, by = "age_interval") %>%
  filter(!(is.na(age_interval))) %>%
  mutate(ct_raw_age_deaths = ifelse(is.na(ct_raw_age_deaths), 0, ct_raw_age_deaths)) %>%
  mutate(ct_case_fatality_rate = ct_raw_age_deaths / ct_raw_age_confirmed_cases * 100) %>% pull(ct_case_fatality_rate)

cct_demographic <- cct_mid_year_2019_pop_est %>% 
  mutate(age_interval = NORM_AGE_COHORT) %>%
  group_by(age_interval) %>% 
  summarise(cct_population = sum(AMOUNT)) %>% 
  ungroup() %>%
  mutate(population_pct = cct_population/sum(cct_population)*100) %>%
  select(-cct_population) %>%
  mutate(rate_pct = ct_age_fatality_rate) %>%
  mutate(population = "CCT Pop %",
         fatal_label = "CCT Case Fatality Rate %") 

# RSA total confirmed
rsa_total_confirmed <- rsa_provincial_ts_confirmed %>% select(YYYYMMDD, total) %>% 
  rename(rsa_confirmed = total,
         report_date = YYYYMMDD) %>% 
  mutate(report_date = as_date(report_date))  

# WC_total_confirmed
wc_total_confirmed <- rsa_provincial_ts_confirmed %>% select(YYYYMMDD, WC) %>% rename(report_date = YYYYMMDD,
                                                                                      wc_confirmed = WC)

# Global total confirmed
global_total_confirmed <- global_ts_sorted_confirmed %>% 
  select(-report_date) %>% 
  rowSums(.) %>% 
  enframe(., value = "global_confirmed") %>% 
  mutate(report_date = as_date(global_ts_sorted_confirmed$report_date)) %>% 
  select(-name)

# Global total deaths
global_total_deaths <- global_ts_sorted_deaths %>% 
  select(-report_date) %>% 
  rowSums(.) %>% 
  enframe(., value = "global_deaths") %>% 
  mutate(report_date = as_date(global_ts_sorted_deaths$report_date)) %>% 
  select(-name)

# Prepare announcements data
# covid_general_announcements <- covid_general_announcements %>% 
#   mutate(Date = dmy(Date))
# 
# covid_general_announcements_pretty <- covid_general_announcements %>% mutate(pretty_text = paste("</br> Entity: ", Entity,
#                                                                                                  "</br> Headline: ", Decision_Title,
#                                                                                                  "</br> Detail: </br> ", Decision_Description,
#                                                                                                  sep = "")) %>% select(Date, pretty_text)
# covid_general_announcements_by_date <- covid_general_announcements_pretty %>% 
#   group_by(Date) %>% 
#   summarise(pretty_text = paste0(pretty_text, collapse = "</br> ---------------- </br>")) %>%
#   ungroup()
# 
# announcements_timeseries <- left_join(rsa_total_confirmed, global_total_confirmed, by = "report_date") %>% 
#   left_join(., wc_total_confirmed, by = "report_date") %>%
#   left_join(., global_total_deaths, by = "report_date") %>% 
#   left_join(., covid_general_announcements_by_date, by = c("report_date" = "Date"))

# PULL IN AND PREPARE GEO DATA ==========================================
# THis code moved to covid-19-data repo

# Enrich with other spatial data

# Add COVID data ----
# TODO ADD REAL WARD LEVEL COVID STATS

# VALUEBOXES =============================================================
latest_values <- listN(ct_latest_update,
                       ct_latest_confirmed,
                       wc_latest_update,
                       wc_latest_confirmed,
                       rsa_latest_update,
                       rsa_latest_tested, 
                       rsa_latest_confirmed, 
                       rsa_latest_deaths,
                  global_last_updated,
                  global_last_confirmed_val,
                  global_last_deaths_val)

write(
  toJSON(latest_values), 
  file.path(getwd(), public_destdir,"latest_values.json")
  )

latest_private_values <- append(latest_values,
                                listN(ct_latest_update,
                                ct_latest_confirmed,
                                ct_latest_deaths))

write(
  toJSON(latest_private_values), 
  file.path(getwd(), private_destdir,"latest_values.json")
)

# HTML WIDGETS ============================================================
# Expected future trajectory ---------------
future_trajectory <- global_ts_since_100 %>% 
  mutate(MEDIAN = median_values[,1],
         UPPER_QUARTILE = upper_quartile_values[,1],
         LOWER_QUARTILE = lower_quartile_values[,1]) %>%
  drop_na(MEDIAN) %>%
  select(-days_since_passed_100) %>%
  ts(., start =1, end = nrow(.), frequency = 1) %>%
    dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.4,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) %>%
  #dyAxis(name="x",
  #       axisLabelFormatter = "function(d){ return d.getFullyear() }") %>% 
  dySeries(c("LOWER_QUARTILE", "MEDIAN", "UPPER_QUARTILE"), 
           label = "Quartile Values",
           strokeWidth = 4,
           strokePattern = "dashed",
           color = "blue") %>%
  dySeries(name = "South Africa",  color = "red", label = "South Africa", strokeWidth = 5)
save_widget(future_trajectory, public_destdir)

future_trajectory_log <- future_trajectory %>% dyOptions(logscale = TRUE)
save_widget(future_trajectory_log, public_destdir)

# Expected future trajectory deaths ---------------
countries_this_far_deaths <- global_deaths_since_25 %>% 
  select(-days_since_passed_25) %>% 
  apply(., MARGIN = 1, function(x) sum(!is.na(x)))

median_values_deaths <- global_deaths_since_25 %>% 
  select(-days_since_passed_25) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~ median(., na.rm = T))) %>%
  t() 

lower_quartile_values_deaths <- global_deaths_since_25 %>% select(-days_since_passed_25) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~quantile(., probs = 0.25, na.rm = T))) %>%
  t() 

upper_quartile_values_deaths <- global_deaths_since_25 %>% select(-days_since_passed_25) %>%
  t() %>% as_tibble(.name_repair = "universal") %>%
  summarise_all(list(~quantile(., probs = 0.75, na.rm = T))) %>%
  t() 

values_to_drop_deaths <- ifelse(countries_this_far_deaths < 2, NA, 1)

median_values_deaths <- median_values_deaths * values_to_drop_deaths

future_deaths_trajectory <- global_deaths_since_25 %>% 
  mutate(MEDIAN = median_values_deaths[,1],
         UPPER_QUARTILE = upper_quartile_values_deaths[,1],
         LOWER_QUARTILE = lower_quartile_values_deaths[,1]) %>%
  drop_na(MEDIAN) %>%
  select(-days_since_passed_25) %>%
  ts(., start =1, end = nrow(.), frequency = 1) %>%
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.4,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) %>%
  dyAxis(name="x", label = "Days since 25 deaths") %>%
  #       axisLabelFormatter = "function(d){ return d.getFullyear() }") %>% 
  dySeries(c("LOWER_QUARTILE", "MEDIAN", "UPPER_QUARTILE"), 
           label = "Quartile Values",
           strokeWidth = 4,
           strokePattern = "dashed",
           color = "blue") %>%
  dySeries(name = "South Africa",  color = "red", label = "South Africa", strokeWidth = 5)
save_widget(future_deaths_trajectory, public_destdir)

future_deaths_trajectory_log <- future_deaths_trajectory %>% dyOptions(logscale = TRUE)
save_widget(future_deaths_trajectory_log, public_destdir)

# Expected future trajectory deaths per million ---------------
global_country_pop <- global_pop_raw %>% 
  group_by(NAME) %>% 
  summarise(population = sum(POP)) %>% 
  ungroup()

deaths_per_million_trajectory <-   global_deaths_since_25 %>% 
  rownames_to_column(var = "day") %>% mutate(day = as.numeric(day)) %>%
  select(-days_since_passed_25) %>% gather(key = "country", value = "deaths", -day)

deaths_per_million_trajectory <- left_join(deaths_per_million_trajectory, 
            global_country_pop, by = c("country" = "NAME")) %>% 
  mutate(deaths_per_million = deaths / population * 10^6) %>% select(day, country, deaths_per_million) %>% 
  spread(key = "country", value = "deaths_per_million") %>% arrange(day) %>% select(-day)

deaths_per_million_trajectory <- deaths_per_million_trajectory %>%
  ts(., start =1, end = nrow(.), frequency = 1) %>%
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.4,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) %>%
  dyAxis(name="x", label = "Days since 25 deaths") %>%
  dySeries(name = "South Africa",  color = "red", label = "South Africa", strokeWidth = 5)
save_widget(deaths_per_million_trajectory, public_destdir)

deaths_per_million_trajectory_log <- deaths_per_million_trajectory %>% dyOptions(logscale = TRUE)
save_widget(deaths_per_million_trajectory_log, public_destdir)

# r rsa_tests_vs_cases -------
rsa_tests_vs_cases <- left_join(sa_ts_confirmed, 
                                covid19za_timeline_testing %>% 
                                  group_by(YYYYMMDD) %>% 
                                  summarize(cumulative_tests = 
                                              max(cumulative_tests)) %>% 
                                  ungroup(), 
                                by = "YYYYMMDD") %>% 
  drop_na() %>%
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE,
            logscale = FALSE,
            connectSeparatedPoints = TRUE) 
save_widget(rsa_tests_vs_cases, public_destdir)

rsa_tests_vs_cases_log <- rsa_tests_vs_cases %>% dyOptions(logscale = TRUE)
save_widget(rsa_tests_vs_cases_log, public_destdir)

# rsa_tasts_vs_cases_rebased -------------
rsa_tests_vs_cases_rebased <- rsa_tests_vs_cases %>% dyRebase(value = 100)
save_widget(rsa_tests_vs_cases_rebased, public_destdir)

# rsa_transmission_type_timeseries --------
rsa_transmission_type_timeseries <- rsa_confirmed_by_type %>%
df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) 

save_widget(rsa_transmission_type_timeseries, public_destdir)

rsa_transmission_type_timeseries_log <- rsa_transmission_type_timeseries %>% dyOptions(logscale = TRUE)
save_widget(rsa_transmission_type_timeseries_log, public_destdir)


# rsa_provincial_timeseries --------------
rsa_provincial_confirmed_timeseries <- rsa_provincial_ts_confirmed %>% 
  select(-total, -source) %>%
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dySeries(name = "WC", label = "WC", color = "red", strokeWidth = 5) %>%
  dyOptions(stackedGraph = TRUE, connectSeparatedPoints = TRUE) 
save_widget(rsa_provincial_confirmed_timeseries, public_destdir)

rsa_provincial_confirmed_timeseries_log <- rsa_provincial_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(rsa_provincial_confirmed_timeseries_log, public_destdir)


# ct_daily_count_timeseries --------------
ct_all_cases_parsed <- ct_all_cases %>% mutate_at(vars(Admission_date, Date_of_ICU_admission, date_of_death), dmy)

ct_daily_counts <- ct_all_cases_parsed %>% 
  group_by(date_of_diagnosis1) %>% 
  summarise(cases = sum(!is.na(date_of_diagnosis1)),
            deaths = sum(!is.na(date_of_death)),
            gen_admissions = sum(!is.na(Admission_date)),
            icu_admissions = sum(!is.na(Date_of_ICU_admission))) %>% 
  ungroup() %>% 
  rename(date = date_of_diagnosis1)

ct_cumulative_daily_counts <- ct_daily_counts %>% 
  mutate(cumulative_cases = cumsum(cases),
         cumulative_gen_admissions = cumsum(gen_admissions),
         cumulative_icu_admissions = cumsum(icu_admissions),
         cumulative_deaths = cumsum(deaths)) %>% 
  mutate(cases_under_14_days = rollsum(cases, 14, fill = NA, align = "right")) %>% 
  mutate(cases_under_14_days = ifelse(is.na(cases_under_14_days), cumulative_cases, cases_under_14_days))
  
# ct_cumulative count_timeseries plot ------------
ct_confirmed_timeseries <- ct_cumulative_daily_counts %>% 
  select(-cases, -deaths, -gen_admissions, -icu_admissions) %>%
  rename(`Cumulative Cases` = cumulative_cases,
         `Cumulative General Admissions` = cumulative_gen_admissions,
         `Cumulative ICU Admissions` = cumulative_icu_admissions,
         `Cumulative Deaths` = cumulative_deaths,
         `Cases less than 14 days old` = cases_under_14_days) %>%
  df_as_xts("date") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) 
save_widget(ct_confirmed_timeseries, private_destdir)

ct_confirmed_timeseries_log <- ct_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(ct_confirmed_timeseries_log, private_destdir)

# ct subdistrict count time series -----------------------------
ct_subdistrict_daily_confirmed_cases  <- ct_all_cases_parsed %>% 
  rename(date = date_of_diagnosis1) %>%
  group_by(date, subdistrict) %>% 
  summarise(count = n()) %>% 
  ungroup() 

# Make sure all dates are included in the melted dataframe
spread_ct_subdistrict_daily_confirmed_cases <-  ct_subdistrict_daily_confirmed_cases %>% spread(key = subdistrict, value = count) 
spread_ct_subdistrict_daily_confirmed_cases[is.na(spread_ct_subdistrict_daily_confirmed_cases)] <- 0
ct_subdistrict_daily_confirmed_cases <- spread_ct_subdistrict_daily_confirmed_cases %>% gather(key = "subdistrict", value = count, -date)

every_nth = function(n) {
  return(function(x) {x[c(TRUE, rep(FALSE, n - 1))]})
}

cct_subdistrict_confirmed_cases_bar_chart <- ct_subdistrict_daily_confirmed_cases %>% 
  ggplot(aes(fill=subdistrict, y=count, x=as.character(date))) +
  geom_col(position = position_dodge2(width = 0.9, preserve = "single")) +
  scale_color_manual(values=c(rep("white", 17)))+
  theme(legend.position="none") + 
  xlab("") +
  ylab("Daily Confirmed Cases") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  scale_x_discrete(breaks = every_nth(n = 5)) +
  facet_wrap(~subdistrict, ncol = 4)
  
cct_subdistrict_confirmed_cases_bar_chart <- ggplotly(cct_subdistrict_confirmed_cases_bar_chart) %>% plotly::config(displayModeBar = F)  
save_widget(cct_subdistrict_confirmed_cases_bar_chart, private_destdir)

# ct subdistrict presumed active -------------------
ct_subdistrict_daily_active_cases  <- 
  ct_all_cases_parsed %>% 
  rename(date = date_of_diagnosis1) %>%
  group_by(date, subdistrict) %>% 
  summarise(cases = n()) %>% 
  ungroup() %>% 
  spread(key = subdistrict, value = cases) %>% 
  replace(is.na(.), 0) %>% 
  gather(key = "subdistrict", value = "cases", -date) %>%
  group_by(subdistrict) %>% 
  arrange(date) %>% 
  mutate(cumulative_cases = cumsum(cases)) %>%
  mutate(cases_under_14_days = rollsum(cases, 14, fill = NA, align = "right")) %>% 
  mutate(cases_under_14_days = ifelse(is.na(cases_under_14_days), cumulative_cases, cases_under_14_days)) %>% ungroup() %>% select(date, subdistrict, cases_under_14_days)

ct_subdistrict_cumulative_daily_counts <- ct_all_cases_parsed %>%   
  rename(date = date_of_diagnosis1) %>%
  group_by(date, subdistrict)  %>% 
  summarise(cases = sum(!is.na(date)),
            deaths = sum(!is.na(date_of_death)),
            gen_admissions = sum(!is.na(Admission_date)),
            icu_admissions = sum(!is.na(Date_of_ICU_admission))) %>% 
  ungroup() 

ct_subdistrict_cumulative_daily_counts <- left_join(ct_subdistrict_daily_active_cases, 
                                                    ct_subdistrict_cumulative_daily_counts, 
                                                    by = c("date" = "date",  "subdistrict" = "subdistrict")) %>%   
  replace(is.na(.), 0) %>%
  group_by(subdistrict) %>% 
  arrange(date) %>% 
  mutate(cumulative_cases = cumsum(cases),
         cumulative_gen_admissions = cumsum(gen_admissions),
         cumulative_icu_admissions = cumsum(icu_admissions),
         cumulative_deaths = cumsum(deaths)) %>% 
  rename(presumed_active = cases_under_14_days) %>% 
  mutate(presumed_recovered = cumulative_cases - cumulative_deaths - presumed_active) %>%
  ungroup() 


for (subdist in unique(ct_subdistrict_cumulative_daily_counts$subdistrict)) {
  subdist_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% filter(subdistrict == subdist) 
  p <- plot_ly(subdist_cumulative_daily_counts, 
               x = ~date, 
               y = ~presumed_active, 
               type = 'bar', 
               name = 'Presumed Active',
               marker = list(color = 'rgba(55, 128, 191, 0.7)'))
  p <- p %>% add_trace(y = ~presumed_recovered, name = 'Presumed Recovered', 
                       marker = list(color = 'rgba(50, 171, 96, 0.7)'))
  p <- p %>% add_trace(y = ~cumulative_deaths, name = 'Cumulative Deaths',
                       marker = list(color = 'rgba(219, 64, 82, 0.7)'))
  p <- p %>% layout(yaxis = list(title = 'Count'), barmode = 'stack')
  obj_name <- print(paste("cct_", str_replace_all(str_replace_all(subdist, " ", "_"), "&", ""), "_cumulative_counts", sep = ""))  
  assign(obj_name, p)
  savepath <- file.path(getwd(), private_destdir, 
                        paste(obj_name, "html", sep = "."))
  libdir <- file.path(getwd(), private_destdir, 
                      "libdir")
  saveWidget(get(obj_name), savepath, selfcontained = F, libdir = libdir)
  rm(p)
}

y_upper <- max(ct_subdistrict_cumulative_daily_counts$cumulative_cases)
ct_subdistrict_cumulative_daily_counts_bar_chart <- ct_subdistrict_cumulative_daily_counts %>%
    group_by(subdistrict) %>%
    group_map(.f = ~{          
          ## fill missing levels w/ displ = 0, cyl = first available value 
          #complete(.x, trans, fill = list(displ = 0, cyl = head(.x$cyl, 1))) %>%
          plot_ly(.,  x = ~date, 
               y = ~presumed_active, 
               type = 'bar', 
               name = 'Presumed Active',
               marker = list(color = 'rgba(55, 128, 191, 0.7)'), 
              showlegend = (.y == "Eastern"), legendgroup = "group1") %>%
           add_trace(y = ~presumed_recovered, name = 'Presumed Recovered <br> * 14 days since diagnosis', 
                     marker = list(color = 'rgba(50, 171, 96, 0.7)')) %>%
           add_trace(y = ~cumulative_deaths, name = 'Cumulative Deaths',
                     marker = list(color = 'rgba(219, 64, 82, 0.7)')) %>%
        layout(yaxis = list(range = c(0, y_upper)),
               barmode = 'stack') %>%
        add_annotations(text = as.character(.y), x = 0.05, y = 0.95, yref = "paper", xref = "paper",
                        xanchor = "left", yanchor = "top", showarrow = FALSE, font = list(size = 14), align = "left")
      
        }) %>%
  subplot(margin = 0.01, 
          shareX = TRUE, 
          shareY = TRUE, 
          nrows = 4, 
          titleX = F, 
          titleY = F)

save_widget(ct_subdistrict_cumulative_daily_counts_bar_chart, private_destdir)

# ct_subdistrict_daily_counts -------------
for (sub in unique(ct_subdistrict_cumulative_daily_counts$subdistrict)) {
  plt <- ct_subdistrict_cumulative_daily_counts %>% 
    filter(`subdistrict` == sub) %>%  ggplot(aes(fill=subdistrict, y=cases, as.character(x=date))) +
    geom_col(position = position_dodge2(width = 0.9, preserve = "single")) +
    scale_color_manual(values=c(rep("white", 17)))+
    xlab("") +
    ylab("Daily Confirmed Cases") +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
    scale_x_discrete(breaks = every_nth(n = 5))
  
  plt <- ggplotly(plt) %>% plotly::config(displayModeBar = F)
  plt$sizingPolicy$padding = 0
  plt$sizingPolicy$browser$padding = 0
  plt$sizingPolicy$viewer$padding = 0
  
  obj_name <- print(paste("cct_", sub, "_confirmed_timeseries", sep = ""))  
  assign(obj_name, plt)
  savepath <- file.path(getwd(), private_destdir, 
                        paste(obj_name, "html", sep = "."))
  libdir <- file.path(getwd(), private_destdir, 
                      "libdir")
  saveWidget(get(obj_name), savepath, selfcontained = F, libdir = libdir)
  rm(plt)
}

# wc_daily_count_timeseries --------------
wc_daily_confirmed_cases <- wc_all_cases %>% 
  select(date_of_diagnosis1) %>% 
  group_by(date_of_diagnosis1) %>% 
  summarise(count = n()) %>% ungroup()

# wc_cumulative_count_timeseries --------------
wc_confirmed_timeseries <- wc_daily_confirmed_cases %>% 
  mutate(WC = cumsum(count)) %>% 
  rename(date = date_of_diagnosis1) %>% 
  select(date, WC)

wc_confirmed_timeseries <- wc_confirmed_timeseries %>% 
  select(date, WC) %>%
  df_as_xts("date") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = TRUE, connectSeparatedPoints = TRUE) 
save_widget(wc_confirmed_timeseries, private_destdir)

wc_confirmed_timeseries_log <- wc_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(wc_confirmed_timeseries_log, private_destdir)

# rsa_timeline_testing ----------------------------------
rsa_timeline_testing <- covid19za_timeline_testing %>% 
  select(YYYYMMDD, cumulative_tests) %>%
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE,
            logscale = FALSE, connectSeparatedPoints = TRUE) 
save_widget(rsa_timeline_testing, public_destdir)

rsa_timeline_testing_log <- rsa_timeline_testing %>% dyOptions(logscale = TRUE)
save_widget(rsa_timeline_testing_log, public_destdir)


# rsa_dem_pyramid -------------------------------------
rsa_dem_pyramid <- ggplot(rsa_pop_genders_ages) +
  geom_bar(aes(x=AGE, y=male), fill="#59ABE3", stat="identity") +
  geom_bar(aes(x=AGE, y=-female), fill="#E66551", stat="identity") +
  coord_flip() +
  theme_bw() +
  ylab("Population") +
  xlab("Age") +
  # Kludge, adapt the -3000 and 3000 according to the breaks
  scale_y_continuous(labels=abs, breaks=c(-5000, 0, 5000)) +
  theme(#axis.title.x=element_blank(),
    axis.text.x=element_blank())#,
#axis.ticks.x=element_blank())
# Alternative with the values on the bars instead of on the X axis
#geom_text(aes(label=Homens,   x=Idade, y=Homens  ),  position="dodge") +
#geom_text(aes(label=Mulheres, x=Idade, y=Mulheres),  position="dodge") +
#theme(axis.title.x=element_blank(), axis.text.x=element_blank(), axis.ticks.x=element_blank())

rsa_dem_pyramid <- ggplotly(rsa_dem_pyramid) %>% plotly::config(displayModeBar = F)  
save_widget(rsa_dem_pyramid, public_destdir)

# rsa demographic mortality plot ---------------------
rsa_demographic_mortality_plot <- 
  china_demographic %>% mutate(rate_pct = -rate_pct,
                                population_pct = -population_pct) %>%
    rbind(., rsa_demographic) %>%
    ggplot(aes(x = age_interval, y = population_pct)) +
    geom_bar(aes(fill = population), 
             alpha = 6/10,
             stat = "identity") +
    geom_bar(aes(y = rate_pct, fill = fatal_label), 
             
             width = 0.5, 
             stat = "identity", group = 1) +
    
    scale_fill_manual(values = c(`SA Pop %` = "#D55E00", 
                                 `China Pop %` = "#E69F00", 
                                 `SA Fatality Rate %` = "#D55E00",
                                 `China Case Fatality Rate %` = "#E69F00"), 
                      name="") +
    coord_flip() +
    labs(x = "", y = "RSA Demographics vs COVID Case Fatalities (%)") +
    theme_bw()
  
rsa_demographic_mortality_plot <- ggplotly(rsa_demographic_mortality_plot)  %>% plotly::config(displayModeBar = F)  
save_widget(rsa_demographic_mortality_plot, public_destdir)

# cape town demographic mortality plot ---------------------
cct_demographic_case_fatality_plot <- 
  china_demographic %>% mutate(rate_pct = -rate_pct,
                               population_pct = -population_pct) %>%
  rbind(., cct_demographic) %>%
  ggplot(aes(x = age_interval, y = population_pct)) +
  geom_bar(aes(fill = population), 
           alpha = 6/10,
           stat = "identity") +
  geom_bar(aes(y = rate_pct, fill = fatal_label), 
           
           width = 0.5, 
           stat = "identity", group = 1) +
  
  scale_fill_manual(values = c(`CCT Pop %` = "#D55E00", 
                               `China Pop %` = "#E69F00", 
                               `CCT Case Fatality Rate %` = "#D55E00",
                               `China Case Fatality Rate %` = "#E69F00"), 
                    name="") +
  coord_flip() +
  labs(x = "", y = "") +
  theme_bw() +
  theme(legend.position="bottom")

cct_demographic_case_fatality_plot <- ggplotly(cct_demographic_case_fatality_plot)  %>% 
  plotly::config(displayModeBar = F)  %>% 
  layout(legend = list(orientation = "h",y = 0, x = 0))

save_widget(cct_demographic_case_fatality_plot, private_destdir)

# china demographic mortality plot ---------------------
china_demographic_mortality_plot <- 
  china_demographic %>% mutate(rate_pct = -rate_pct,
                               population_pct = -population_pct) %>%
  ggplot(aes(x = age_interval, y = population_pct)) +
  geom_bar(aes(fill = population), 
           alpha = 6/10,
           stat = "identity") +
  geom_bar(aes(y = rate_pct, fill = fatal_label), 
           width = 0.5, 
           stat = "identity", group = 1) +
  scale_fill_manual(values = c(`SA Pop %` = "#D55E00", 
                               `China Pop %` = "#E69F00", 
                               `SA Fatality Rate %` = "#D55E00",
                               `China Case Fatality Rate %` = "#E69F00"), 
                    name="") +
  coord_flip() +
  labs(x = "", y = "China Age Demographics and COVID Case Fatality Rate (%)") +
  theme_bw()

china_demographic_mortality_plot <- ggplotly(china_demographic_mortality_plot)  %>% plotly::config(displayModeBar = F)  
save_widget(china_demographic_mortality_plot, public_destdir)

# global_timeline_confirmed ----------------------------
global_timeline_confirmed <- global_ts_sorted_confirmed %>% 
  df_as_xts("report_date") %>% 
  dygraph() %>%
  dyLegend(show = "follow") %>%
  dyCSS(textConnection("
    .dygraph-legend > span { display: none; }
    .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.2,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = TRUE, strokeWidth = c(1,5), connectSeparatedPoints = TRUE)

save_widget(global_timeline_confirmed, public_destdir)

global_timeline_confirmed_log <- global_timeline_confirmed %>% dyOptions(logscale = TRUE)
save_widget(global_timeline_confirmed_log, public_destdir)


# browsable_global ------------------------------------
browsable_global <- global_latest_data %>% 
  select(country,
         population,
         confirmed,
         deaths,
         incidence_per_1m,
         mortality_per_1m,
         case_fatality_rate_pct) %>%
  DT::datatable(options = list(pageLength = 25))
save_widget(browsable_global, public_destdir)

# global_mortality_boxplot ----------------------------
outliers <- boxplot(case_fatality_rate_pct~maturity,
                    data=global_latest_data
                    
                    , plot=FALSE)$out

global_mortality_data <- global_latest_data %>% filter(!(case_fatality_rate_pct %in% outliers)) %>%
  filter(country != "San Marino") %>% as.data.frame()

global_mortality_boxplot <- bpexploder(data = global_mortality_data,
           settings = list(
             groupVar = "maturity",
             levels = levels(global_mortality_data$maturity),
             yVar = "case_fatality_rate_pct",
             yAxisLabel = "Case Fatality Rate %",
             xAxisLabel = "Cumulative confirmed cases",
             tipText = list(
               country = "country"
             ),
             relativeWidth = 0.75)
)

save_widget(global_mortality_boxplot, public_destdir)

# global ranked fatalities per 1m --------------------------
global_top_fatalities_per_1m <- global_latest_data %>% 
  select(country, mortality_per_1m) %>% 
  filter(rank(desc(mortality_per_1m)) <= 20 | country == "South Africa") %>%
  filter(country != "San Marino") %>% 
  arrange(desc(mortality_per_1m))

global_top_fatalities_per_1m$country <- factor(global_top_fatalities_per_1m$country, levels = global_top_fatalities_per_1m$country)

global_ranked_fatalities_per_1m  <- ggplot(global_top_fatalities_per_1m, aes(country, mortality_per_1m)) + 
  geom_col() + 
  coord_flip()

global_ranked_fatalities_per_1m <- ggplotly(global_ranked_fatalities_per_1m) %>% plotly::config(displayModeBar = F)
save_widget(global_ranked_fatalities_per_1m, public_destdir)

# HR HEATMAP ==========================================================================
# # Munge
# hr_data_complete_functionality <- hr_data_complete %>% mutate(functionality = recode(hr_data_complete$Evaluation, 
#        "We won't be able to perform the required minimum" = 0,
#        "We cannot deliver on daily tasks" = 0.1,
#        "We can do the bare minimum" = 0.3,
#        "Staff can do 50% of their function" = 0.5,              
#        "Staff can do 70 % of their function" = 0.7,              
#        "We can deliver 75% or less of daily tasks" = 0.75,
#        "We can deliver on daily tasks" = 0.9,                   
#        "We can continue as normal" = 1))
# 
# hr_latest_known <- hr_data_complete_functionality %>% 
#   select(`Employee No`, Date, Categories, functionality) %>% 
#   group_by(`Employee No`) %>% 
#   filter(Date == max(Date)) %>% 
#   ungroup() %>% select(-Date, -Categories) %>% rename(attendance = functionality)
# 
# # Make sure to show blanks
# 
# city_people_sf <- city_people %>% filter(!is.na(LocationWkt)) %>% st_as_sf(wkt = "LocationWkt")
# st_crs(city_people_sf) <- st_crs(cct_2016_pop_density)
# city_people_sf
# 
# # Create base heatmap
# org_status <- city_people %>% 
#   select(StaffNumber, Section, Branch, Department, Directorate)  %>% 
#   mutate(StaffNumber = as.character(StaffNumber)) 
# 
# org_status <- org_status  %>% left_join(., hr_latest_known, by = c("StaffNumber" = "Employee No"))
# 
# org_status <- org_status %>% filter(!is.na(attendance))
# 
# section_level_org_status <- org_status %>% 
#   filter(!is.na(Section)) %>% 
#   group_by(Section) %>% 
#   summarise(section_attendance_pct = (sum(attendance, na.rm = T) / length(attendance))) %>% 
#   ungroup()
# 
# branch_level_org_status <- org_status %>% 
#   filter(!is.na(Branch)) %>% 
#   group_by(Branch) %>% 
#   summarise(branch_attendance_pct = (sum(attendance, na.rm = T) / length(attendance))) %>% 
#   ungroup()
# 
# dept_level_org_status <- org_status %>% 
#   filter(!is.na(Department)) %>% 
#   group_by(Department) %>% 
#   summarise(dept_attendance_pct = (sum(attendance, na.rm = T) / length(attendance))) %>% 
#   ungroup()
# 
# dir_level_org_status <- org_status %>% 
#   filter(!is.na(Directorate)) %>% 
#   group_by(Directorate) %>% 
#   summarise(dir_attendance_pct = (sum(attendance, na.rm = T) / length(attendance))) %>% 
#   ungroup()
# 
# org_view <- org_status %>% 
#   select(-StaffNumber, -attendance) %>% 
#   unique() %>% 
#   filter(!is.na(Section)) %>%
#   left_join(., section_level_org_status, by = c("Section")) %>%
#   left_join(., branch_level_org_status, by = c("Branch")) %>%
#   left_join(., dept_level_org_status, by = c("Department")) %>%
#   left_join(., dir_level_org_status, by = c("Directorate"))
# 
# org_xyz <- org_view  %>% 
#   rename(`Section Level` = section_attendance_pct ,
#          `Branch Level` = branch_attendance_pct,
#          `Dept Level` = dept_attendance_pct,
#          `Dir Level` = dir_attendance_pct) %>%
#   gather(key = hierarchy, 
#          value = attendance, 
#          -Section, -Branch, -Department, -Directorate)  
# 
# hierarchy_order <- c("Section Level", "Branch Level", "Dept Level", "Dir Level")
# org_xyz$hierarchy <- factor(x = org_xyz$hierarchy,
#                                levels = hierarchy_order, 
#                                ordered = TRUE)
# 
# # # GGPLOT VERSION
# # p <- ggplot(org_xyz, aes(Section, hierarchy, fill= attendance)) + 
# #   geom_tile(color="black", size=0.1) +
# #   scale_fill_distiller(palette = "RdBu", direction = 1) +
# #   xlab(label = "Attendance %") + 
# #   theme(axis.title.x=element_blank(),
# #         axis.text.x=element_blank(),
# #         axis.ticks.x=element_blank()) +
# #   #facet_grid(~ Directorate, switch = "x", scales = "free_x", space = "free_x") +
# #   facet_wrap(~ Directorate, scales = "free_x") +
# #   theme(strip.text.x = element_text(size = 8, colour = "red", angle = 90)) 
# # 
# # p
# # 
# #   
# # ggplotly(p)
# # p
# # fig <- style(ggplotly(p), xgap = 5, ygap = 5, hoverinfo = "y", traces = 2)
# # fig
# 
# # PLOTLY VERSION
# library(plotly)
# library(stringr)
# #org_xyz <- org_xyz %>% 
# #  mutate(text = paste0("x: ", Section, "\n", "y: ", Branch, "\n", "Value: ", Department, "\n", "What else?"))
# 
# for (directorate in unique(org_xyz$Directorate)) {
#   dir_xyz <- org_xyz %>% filter(Directorate == directorate) 
#   dir_plotly_view <- dir_xyz %>% spread(key = hierarchy, value = attendance)
#   plotly_matrix <- dir_plotly_view %>% select(-Section, -Branch, -Department, -Directorate) %>% t()
#   p <- plot_ly(x = dir_plotly_view$Section,
#                y = c("Section", "Branch", "Department", "Directorate"), 
#                z = plotly_matrix,
#                colors = colorRamp(c("red", "blue")),
#                type = "heatmap") %>% 
#     layout(xaxis= list(showticklabels = FALSE)) %>%
#     style(xgap = 1, ygap = 1)
#   obj_name <- print(paste("cct_", str_replace_all(str_replace_all(directorate, " ", "_"), "&", ""), "_hr_attendance", sep = ""))  
#   assign(obj_name, p)
#   savepath <- file.path(getwd(), private_destdir, 
#                         paste(obj_name, "html", sep = "."))
#   libdir <- file.path(getwd(), private_destdir, 
#                       "libdir")
#   saveWidget(get(obj_name), savepath, selfcontained = F, libdir = libdir)
#   rm(p)
# }
# 
# subplot(cct_WATER_AND_WASTE_hr_attendance,
#         cct_COMMUNITY_SERVICES_and_HEALTH_hr_attendance,
#         cct_CORPORATE_SERVICES_hr_attendance,
#         cct_ECONOMIC_OPPORTUNITIES_ASSET_MANAGEMENT_hr_attendance,
#         cct_SPATIAL_PLANNING_AND_ENVIRONMENT_hr_attendance, 
#         margin = 0.01, 
#         shareX = F, 
#         shareY = TRUE, 
#         nrows = 4, 
#         titleX = FALSE, 
#         titleY = FALSE)
# 

# WC MODEL OUTPUT ==============================================================

wc_model_latest_date <- max(wc_model_data$ForecastDate)

wc_model_latest_date_list <- listN(wc_model_latest_date)

write(
  toJSON(wc_model_latest_date_list), 
  file.path(getwd(), private_destdir,"latest_wc_model_update.json")
)

wc_model_latest <- wc_model_data %>% filter(ForecastDate == wc_model_latest_date) %>% select(-ForecastDate) %>% mutate(Scenario = tolower(Scenario))

wc_model_latest_cumulative <- wc_model_latest %>% 
  arrange(TimeInterval) %>% 
  group_by(Scenario) %>% 
  mutate(TotalDeaths = cumsum(NewDeaths),
         TotalInfections = cumsum(NewInfections)) %>% 
  ungroup() %>% 
  mutate(CaseFatalityRate = TotalDeaths / TotalInfections)

wc_model_latest_default <- wc_model_latest_cumulative %>% 
  filter(Scenario == "default")

wc_model_latest_disease_figures <- plot_ly(wc_model_latest_default,
                                           x = ~TimeInterval,
                                           y = ~NewInfections, type = 'bar',
                                           name = 'Daily New Infections') %>%
  add_trace(y = ~TotalDeaths, name = 'Modeled Cumulative Deaths') %>%
  layout(legend = list(orientation = 'h'), xaxis = list(title = ""))

save_widget(wc_model_latest_disease_figures, private_destdir)

wc_model_latest_hospital_figures <- wc_model_latest_default %>% 
  plot_ly() %>% 
  add_trace(x = ~TimeInterval, y = ~TotalDeaths, name = 'Modeled Cumulative Deaths', type = 'scatter', mode = 'lines', line = list(color = 'rgb(205, 12, 24)')) %>%
  add_trace(x = ~TimeInterval, y = ~NewDeaths, name = 'Daily New Deaths', type = 'bar', marker = list(color = 'rgb(205, 12, 24)')) %>%
  add_trace(x = ~TimeInterval, y = ~NewGeneralAdmissions, name = 'Daily New General Admissions', type = 'bar', marker = list(color = 'rgb(22, 96, 167)')) %>%
  add_trace(x = ~TimeInterval, y = ~NewICUAdmissions, name = 'Daily New ICU Admissions', type = 'bar', marker = list(color = 'rgb(255, 153, 51)')) %>%
  add_trace(x = ~TimeInterval, y = ~GeneralBedNeed, name = 'Daily General Beds Needed', type = 'scatter', mode = 'lines', line = list(color = 'rgb(22, 96, 167)')) %>%
  add_trace(x = ~TimeInterval, y = ~ICUBedNeed, name = 'Daily ICU Beds Needed', type = 'scatter', mode = 'lines', line = list(color = 'rgb(255, 153, 51)')) %>%
  layout(legend = list(orientation = 'h'), xaxis = list(title = ""), yaxis = list(title = ""))
save_widget(wc_model_latest_hospital_figures, private_destdir)

# MAPS =========================================================================

# ct_heatmap ------------------------
bins <- c(0, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, Inf)
pal <- colorBin("Blues", domain = cct_2016_pop_density$`X2016_POP_DENSITY_KM2`, bins = bins)

labels <- sprintf(
  "<strong>%s</strong> <br/>%g people / km<sup>2</sup> <br/>%g residents",
  paste("Ward", cct_2016_pop_density$WARD_NAME), 
  cct_2016_pop_density$`X2016_POP_DENSITY_KM2`, 
  cct_2016_pop_density$`X2016_POP`) %>% 
  lapply(htmltools::HTML)

ct_heatmap <- leaflet(cct_2016_pop_density) %>%  
  addProviderTiles("Stamen.TonerLite", options = tileOptions(opacity=0.4), group="Map") %>%
  addProviderTiles('Esri.WorldImagery', options = tileOptions(opacity=0.4), group="Satellite") %>% 
  setView(lat = -33.9249, lng = 18.4241, zoom = 10L) %>%
  # ward polys
  addPolygons(fillColor = ~pal(`X2016_POP_DENSITY_KM2`),
              weight = 0.5,
              opacity = .6,
              color = "white",
              dashArray = "3",
              fillOpacity = 0.7,
              highlight = highlightOptions(
                weight = 5,
                color = "#666",
                dashArray = "",
                fillOpacity = 0.7,
                bringToFront = TRUE),
              label = labels,
              labelOptions = labelOptions(
                style = list("font-weight" = "normal", padding = "3px 8px"),
                textsize = "15px",
                direction = "auto")) %>%
  addLegend(pal = pal, title = "Pop. density 2016",
            values = ~`X2016_POP_DENSITY_KM2`, opacity = 0.7, 
            position = "bottomright") %>%
  # control
  addLayersControl(
    baseGroups = c("Map",
                   "Satellite"),
    options = layersControlOptions(collapsed = FALSE)) 

save_widget(ct_heatmap, public_destdir)

# Hex l7 table with sparklines -----------------
# 
# # Construct complete timeseries
# ct_geom_daily_stats <- ct_all_cases_parsed %>% 
#   group_by(hex_l7, date_of_diagnosis1) %>%
#   summarise(daily_cases = n(),
#             daily_hospitalised = sum(!is.na(Admission_date)),
#             daily_ICU = sum(!is.na(Date_of_ICU_admission)),
#             daily_deaths  = sum(!is.na(date_of_death)))
# 
# day_series <- seq(min(ct_all_cases_parsed$date_of_diagnosis1),
#                   max(ct_all_cases_parsed$date_of_diagnosis1), 
#                   by = "day") 
# 
# geom_series <- cct_hex_polygons_7
# geom_series_names <- geom_series$index
# 
# geom_timeseries <- data.frame(matrix(ncol = length(day_series), nrow = length(geom_series_names)))
# colnames(geom_timeseries) <- day_series
# geom_timeseries$index <- geom_series_names
# 
# geom_timeseries <- geom_timeseries %>% as_tibble() %>% 
#   gather(key = "date", value = "dummy", -index) %>% 
#   mutate(date = ymd(date)) %>% 
#   select(-dummy)
# 
# geom_timeseries <- geom_timeseries %>% 
#   left_join(., ct_geom_daily_stats, by = c("index" = "hex_l7",  "date" = "date_of_diagnosis1")) %>% 
#   arrange(date)
# 
# geom_timeseries[is.na(geom_timeseries)] <- 0
# 
# geom_latest <- geom_timeseries %>% 
#   group_by(index) %>% 
#   mutate(CumulativeCases = cumsum(daily_cases)) %>%
#   summarise(`Cumulative Cases` = sum(daily_cases),
#             `Cumulative Admitted` = sum(daily_hospitalised),
#             `Cumulative ICU` = sum(daily_ICU),
#             `Cumulative Deaths` = sum(daily_deaths)) 
# 
# geom_series <- geom_timeseries %>% 
#   group_by(index) %>% 
#   mutate(CumulativeCases = cumsum(daily_cases)) %>%
#   summarise(Cases = paste(daily_cases, collapse = ","),
#             Admitted = paste(daily_hospitalised, collapse = ","),
#             ICU = paste(daily_ICU, collapse = ","),
#             Deaths = paste(daily_deaths, collapse = ",")) 
# 
# # Some js custom functions. INcluded line as well in case you want to use instead
# line_string <- "type: 'line', lineColor: 'black', fillColor: '#ccc', highlightLineColor: 'orange', highlightSpotColor: 'orange'"
# bar_string <- "type: 'bar', barColor: 'orange', negBarColor: 'purple', highlightColor: 'black'"
# js <- "function(data, type, full){ return '<span class=sparkLine>' + data + '</span>' }"
# js_bar <- "function(data, type, full){ return '<span class=sparkBar>' + data + '</span>' }"
# cd <- list(list(targets = 2, render = JS(js_bar)))
# cb <- JS(paste0("function (oSettings, json) {
#   $('.sparkLine:not(:has(canvas))').sparkline('html', { ", 
#                 line_string, " });
#   $('.sparkBar:not(:has(canvas))').sparkline('html', { ", 
#                 bar_string, " });
# 
# }"), collapse = "")
# 
# # Complete table
# geom_object <- right_join(cct_hex_polygons_7, 
#            left_join(geom_latest, 
#                      geom_series, by = "index"), 
#            by = "index")
# complete_table_sparkdef <- list(list(targets = 6, render = JS(js_bar)),
#                                 list(targets = 7, render = JS(js_bar)),
#                                 list(targets = 8, render = JS(js_bar)),
#                                 list(targets = 9, render = JS(js_bar)))
# complete_table <- geom_object %>% 
#   st_set_geometry(NULL) %>% datatable(rownames = TRUE,
#                                       options = list(columnDefs = complete_table_sparkdef, 
#                                                      fnDrawCallback = cb, 
#                                                      dom = 'lrt')) 
# complete_table$dependencies <- append(complete_table$dependencies, htmlwidgets:::getDependency("sparkline"))


# Sparkline table per hex 7 -----------------
# # FOR LOOP HERE
# popups <- vector()
# for (sample_index in geom_latest$index) {
#   sample_latest <- geom_latest %>% 
#     filter(index == geom_latest$index[200]) %>% t() %>% as_tibble()
#   
#   sample_series <- geom_series %>% 
#     filter(`index` == pull(sample_latest[1,1])) %>% t() %>% as_tibble()
#   
#   sample_table <- cbind(sample_latest, sample_series) 
#   rownames(sample_table) <- c("Index", "Cases", "Admitted", "ICU", "Deaths")
#   colnames(sample_table) <- c("Cumulative", "Daily New")
# 
#   d5 <- datatable(sample_table, 
#                   rownames = TRUE, 
#                   options = list(columnDefs = cd, fnDrawCallback = cb, dom = '')) 
#   d5$dependencies <- append(d5$dependencies, htmlwidgets:::getDependency("sparkline"))
#   popups <- append(popups, as.character(d5))
#   
# }

# SEND TO MINIO =================================================================
for (filename in list.files(public_destdir, recursive = T)) {
  filepath <- file.path(public_destdir, filename)
  if (file_ext(filepath) != "") {
    print(filename)
    if(dirname(filename) == ".") {
      prefix_override = paste(public_destdir, "/", sep="")
    } else {
      prefix_override = paste(file.path(public_destdir, dirname(filename)), "/", sep="")
    }
    print(prefix_override)
    file_to_minio(filepath,
                  "covid",
                  minio_key,
                  minio_secret,
                  "EDGE",
                  filename_prefix_override = prefix_override)
    
    print("Sent")
  } else {
    print(filepath)
    print("This is a directory - not sending!")
    }
}

for (filename in list.files(private_destdir, recursive = T)) {
  filepath <- file.path(private_destdir, filename)
  if (file_ext(filepath) != "") {
    print(filename)
    if(dirname(filename) == ".") {
      prefix_override = paste(private_destdir, "/", sep="")
    } else {
      prefix_override = paste(file.path(private_destdir, dirname(filename)), "/", sep="")
    }
    print(prefix_override)
    file_to_minio(filepath,
                  "covid",
                  minio_key,
                  minio_secret,
                  "EDGE",
                  filename_prefix_override = prefix_override)
    
    print("Sent")
  } else {
    print(filepath)
    print("This is a directory - not sending!")
  }
}

# Save a copy of the data to .Rdata for dashboard knit
save.image(file = "widgets.RData")
