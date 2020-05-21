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
library(zoo)


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

# read in service requests ----------------
# minio_to_file("service-requests-full.parquet",
#               "service-requests-full",
#               minio_key,
#               minio_secret,
#               "EDGE")
# 
# service_requests <- read_parquet("service-requests-full.parquet")
# unlink("service-requests-full.parquet")

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
rsa_latest_deaths <- covid19za_provincial_timeline_deaths %>% filter(YYYYMMDD == max(YYYYMMDD)) %>%  pull(total) %>% .[1]
rsa_latest_tested <- covid19za_timeline_testing %>% summarise(val = max(cumulative_tests, na.rm = T)) %>% pull(val) %>% .[1]

# Latest values WC ---------------------------------
wc_latest_update <- max(wc_all_cases$Date.of.Diagnosis)
wc_latest_confirmed <- nrow(wc_all_cases)

# Latest values CT
ct_latest_update <- wc_latest_update
ct_latest_confirmed <- nrow(ct_all_cases)
ct_latest_deaths <- ct_all_cases %>% filter(!is.na(Date.of.Death)) %>% nrow() 

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
    select(Agegroup) %>%
    separate(Agegroup, sep = "[ ]", into = c("age"), extra = "drop") %>%
    mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>%
  mutate(age_interval = age_bracket_labels[age_interval]) %>%
  group_by(age_interval) %>%
  summarise(ct_raw_age_confirmed_cases = n()) %>%
  ungroup()

# cape town case fatality pop pyramid ---------------
ct_raw_age_deaths <- ct_all_cases  %>%
  filter(!is.na(Date.of.Death)) %>%
  select(Agegroup) %>% 
  separate(Agegroup, sep = "[ ]", into = c("age"), extra = "drop") %>%
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
ct_all_cases_parsed <- ct_all_cases #%>% mutate_at(vars(Date.of.Diagnosis, Admission.Date, Date.of.ICU.Admission, Discharge.Date, Date.of.Death), dmy)

# ct_daily_counts <- ct_all_cases_parsed %>% 
#   group_by(Date.of.Diagnosis) %>% 
#   summarise(cases = sum(!is.na(Date.of.Diagnosis)),
#             deaths = sum(!is.na(Date.of.Death)),
#             gen_admissions = sum(!is.na(Admission.Date)),
#             icu_admissions = sum(!is.na(Date.of.ICU.Admission))) %>% 
#   ungroup() %>% 
#   rename(date = Date.of.Diagnosis)
# 
# ct_cumulative_daily_counts <- ct_daily_counts %>% 
#   mutate(cumulative_cases = cumsum(cases),
#          cumulative_gen_admissions = cumsum(gen_admissions),
#          cumulative_icu_admissions = cumsum(icu_admissions),
#          cumulative_deaths = cumsum(deaths)) %>% 
#   mutate(cases_under_14_days = rollsum(cases, 14, fill = NA, align = "right")) %>% 
#   mutate(cases_under_14_days = ifelse(is.na(cases_under_14_days), cumulative_cases, cases_under_14_days))
  
# ct_cumulative count_timeseries plot ------------
# NOTE: REMOVED IN FAVOUR OF PLOTLY BAR CHART
# ct_confirmed_timeseries <- ct_cumulative_daily_counts %>% 
#   select(-cases, -deaths, -gen_admissions, -icu_admissions) %>%
#   rename(`Cumulative Cases` = cumulative_cases,
#          `Cumulative General Admissions` = cumulative_gen_admissions,
#          `Cumulative ICU Admissions` = cumulative_icu_admissions,
#          `Cumulative Deaths` = cumulative_deaths,
#          `Cases less than 14 days old` = cases_under_14_days) %>%
#   df_as_xts("date") %>% 
#   dygraph() %>%
#   dyLegend(show = "follow") %>%
#   dyCSS(textConnection("
#     .dygraph-legend > span { display: none; }
#     .dygraph-legend > span.highlight { display: inline; }
#   ")) %>%
#   dyHighlight(highlightCircleSize = 5, 
#               highlightSeriesBackgroundAlpha = 0.5,
#               hideOnMouseOut = FALSE) %>%
#   dyRangeSelector(height = 20) %>%
#   dyOptions(stackedGraph = FALSE, connectSeparatedPoints = TRUE) 
# save_widget(ct_confirmed_timeseries, private_destdir)
# 
# ct_confirmed_timeseries_log <- ct_confirmed_timeseries %>% dyOptions(logscale = TRUE)
# save_widget(ct_confirmed_timeseries_log, private_destdir)

# ct subdistrict count time series -----------------------------
ct_subdistrict_daily_confirmed_cases  <- ct_all_cases_parsed %>% 
  rename(date = Date.of.Diagnosis) %>%
  group_by(date, Subdistrict) %>% 
  summarise(count = n()) %>% 
  ungroup() 

# Make sure all dates are included in the melted dataframe
spread_ct_subdistrict_daily_confirmed_cases <-  ct_subdistrict_daily_confirmed_cases %>% spread(key = Subdistrict, value = count) 
spread_ct_subdistrict_daily_confirmed_cases[is.na(spread_ct_subdistrict_daily_confirmed_cases)] <- 0
ct_subdistrict_daily_confirmed_cases <- spread_ct_subdistrict_daily_confirmed_cases %>% gather(key = "Subdistrict", value = count, -date)

every_nth = function(n) {
  return(function(x) {x[c(TRUE, rep(FALSE, n - 1))]})
}

# ct subdistrict presumed active
ct_subdistrict_daily_active_cases  <- 
  ct_all_cases_parsed %>% 
  rename(date = Date.of.Diagnosis) %>%
  group_by(date, Subdistrict) %>% 
  summarise(cases = n()) %>% 
  ungroup() %>% 
  spread(key = Subdistrict, value = cases) %>% 
  replace(is.na(.), 0) %>% 
  gather(key = "Subdistrict", value = "cases", -date) %>%
  group_by(Subdistrict) %>% 
  arrange(date) %>% 
  mutate(cumulative_cases = cumsum(cases)) %>%
  mutate(cases_under_14_days = rollsum(cases, 14, fill = NA, align = "right")) %>% 
  mutate(cases_under_14_days = ifelse(is.na(cases_under_14_days), cumulative_cases, cases_under_14_days)) %>% ungroup() %>% select(date, Subdistrict, cases_under_14_days)

ct_subdistrict_cumulative_daily_counts <- ct_all_cases_parsed %>%   
  rename(date = Date.of.Diagnosis) %>%
  group_by(date, Subdistrict)  %>% 
  summarise(cases = sum(!is.na(date)),
            deaths = sum(!is.na(Date.of.Death)),
            gen_admissions = sum(!is.na(Admission.Date)),
            icu_admissions = sum(!is.na(Date.of.ICU.Admission))) %>% 
  ungroup() 

ct_subdistrict_cumulative_daily_counts <- left_join(ct_subdistrict_daily_active_cases, 
                                                    ct_subdistrict_cumulative_daily_counts, 
                                                    by = c("date" = "date",  "Subdistrict" = "Subdistrict")) %>%   
  replace(is.na(.), 0) %>%
  group_by(Subdistrict) %>% 
  arrange(date) %>% 
  mutate(cumulative_cases = cumsum(cases),
         cumulative_gen_admissions = cumsum(gen_admissions),
         cumulative_icu_admissions = cumsum(icu_admissions),
         cumulative_deaths = cumsum(deaths)) %>% 
  rename(presumed_active = cases_under_14_days) %>% 
  mutate(presumed_recovered = cumulative_cases - cumulative_deaths - presumed_active) %>%
  ungroup() 

# ct cumulative daily counts bar chart ----------------
ct_cumulative_daily_counts_bar_chart <-  ct_subdistrict_cumulative_daily_counts %>%
  plot_ly(.,  x = ~date, 
          y = ~presumed_active, 
          type = 'bar', 
          name = 'Presumed Active',
          marker = list(color = 'rgba(55, 128, 191, 0.7)')) %>%
  add_trace(y = ~presumed_recovered, name = 'Presumed Recovered <br> * 14 days since diagnosis', 
            marker = list(color = 'rgba(50, 171, 96, 0.7)')) %>%
  add_trace(y = ~cumulative_deaths, name = 'Cumulative Deaths',
            marker = list(color = 'rgba(219, 64, 82, 0.7)')) %>%
  layout(barmode = 'stack', legend = list(x = 0.1, y = 0.9))

save_widget(ct_cumulative_daily_counts_bar_chart, private_destdir)

for (subdist in unique(ct_subdistrict_cumulative_daily_counts$Subdistrict)) {
  subdist_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% filter(Subdistrict == subdist) 
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
    group_by(Subdistrict) %>%
    group_map(.f = ~{          
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


# ct daily counts bar chart -------------------------------
ct_daily_counts <-  ct_subdistrict_cumulative_daily_counts %>% 
  group_by(date) %>%
  summarise(gen_admissions = sum(gen_admissions),
            cases = sum(cases),
            deaths = sum(deaths),
            icu_admissions = sum(icu_admissions)) %>% 
  ungroup() %>% 
  mutate(rolling_death_5_days = rollmean(deaths, 5, na.pad=TRUE, align="right"),
         rolling_cases_5_days = rollmean(cases, 5, na.pad=TRUE, align="right"))


ct_daily_counts_bar_chart <- ct_daily_counts %>%   
  plot_ly(.,  x = ~date, 
          y = ~cases, 
          type = 'bar', 
          name = 'New Cases',
          marker = list(color = 'rgba(55, 128, 191, 0.7)')) %>%
  add_trace(y = ~gen_admissions, name = 'General Hospital Admissions', 
            marker = list(color = 'rgba(50, 171, 96, 0.7)')) %>%
  add_trace(y = ~icu_admissions, name = 'ICU Admissions', 
            marker = list(color = 'rgba(255,165,0, 0.7)')) %>%
  add_trace(y = ~deaths, name = 'Deaths',
            marker = list(color = 'rgba(219, 64, 82, 0.7)')) %>%
  add_trace(y = ~rolling_death_5_days, name = 'Deaths 5 Day Average',
            type = "scatter", 
            mode = "line", 
            line = list(color = 'rgba(219, 64, 82, 1)'), 
            marker = list(color = 'rgba(219, 64, 82, 1)')) %>%
  add_trace(y = ~rolling_cases_5_days, name = 'New Cases 5 Day Average  <br> * recent dates are underreported',
            type = "scatter", 
            mode = "line", 
            line = list(color = 'rgba(55, 128, 191, 1)'), 
            marker = list(color = 'rgba(55, 128, 191, 1)')) %>%
  layout(barmode = 'stack', legend = list(x = 0.1, y = 0.9))

save_widget(ct_daily_counts_bar_chart, private_destdir)


for (subdist in unique(ct_subdistrict_cumulative_daily_counts$Subdistrict)) {
  subdist_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% 
    filter(Subdistrict == subdist) %>%  
    mutate(rolling_death_5_days = rollmean(deaths, 5, na.pad=TRUE, align="right"),
           rolling_cases_5_days = rollmean(cases, 5, na.pad=TRUE, align="right"))

  p <- subdist_cumulative_daily_counts %>%
    plot_ly(.,  x = ~date, 
            y = ~cases, 
            type = 'bar', 
            name = 'New Cases',
            marker = list(color = 'rgba(55, 128, 191, 0.7)')) %>%
    add_trace(y = ~gen_admissions, name = 'General Hospital Admissions', 
              marker = list(color = 'rgba(50, 171, 96, 0.7)')) %>%
    add_trace(y = ~icu_admissions, name = 'ICU Admissions', 
              marker = list(color = 'rgba(255,165,0, 0.7)')) %>%
    add_trace(y = ~deaths, name = 'Deaths',
              marker = list(color = 'rgba(219, 64, 82, 0.7)')) %>%
    add_trace(y = ~rolling_death_5_days, name = 'Deaths 5 Day Average',
              type = "scatter", 
              mode = "line", 
              line = list(color = 'rgba(219, 64, 82, 1)'), 
              marker = list(color = 'rgba(219, 64, 82, 1)')) %>%
    add_trace(y = ~rolling_cases_5_days, name = 'New Cases 5 Day Average  <br> * recent dates are underreported',
              type = "scatter", 
              mode = "line", 
              line = list(color = 'rgba(55, 128, 191, 1)'), 
              marker = list(color = 'rgba(55, 128, 191, 1)')) %>%
    
    layout(barmode = 'stack')
  obj_name <- print(paste("cct_", str_replace_all(str_replace_all(subdist, " ", "_"), "&", ""), "_daily_counts_bar_chart", sep = ""))  
  assign(obj_name, p)
  savepath <- file.path(getwd(), private_destdir, 
                        paste(obj_name, "html", sep = "."))
  libdir <- file.path(getwd(), private_destdir, 
                      "libdir")
  saveWidget(get(obj_name), savepath, selfcontained = F, libdir = libdir)
  rm(p)
}

y_upper <- max(ct_subdistrict_cumulative_daily_counts$cases + 
  ct_subdistrict_cumulative_daily_counts$deaths + 
  ct_subdistrict_cumulative_daily_counts$gen_admissions + 
  ct_subdistrict_cumulative_daily_counts$icu_admissions)
ct_subdistrict_daily_counts_bar_chart <- ct_subdistrict_cumulative_daily_counts %>%
  group_by(Subdistrict) %>%
  group_map(.f = ~{          
    plot_ly(.,  x = ~date, 
            y = ~cases, 
            type = 'bar', 
            name = 'New Cases',
            marker = list(color = 'rgba(55, 128, 191, 0.7)'),
      showlegend = (.y == "Eastern"), legendgroup = "group1") %>%
      add_trace(y = ~gen_admissions, name = 'General Hospital Admissions', 
                marker = list(color = 'rgba(50, 171, 96, 0.7)')) %>%
      add_trace(y = ~icu_admissions, name = 'ICU Admissions', 
                marker = list(color = 'rgba(255,165,0, 0.7)')) %>%
      add_trace(y = ~deaths, name = 'Deaths',
                marker = list(color = 'rgba(219, 64, 82, 0.7)')) %>%
      layout(yaxis = list(range = c(0, y_upper)),
             barmode = 'stack') %>%
      add_annotations(text = as.character(.y), x = 0.05, y = 0.95, yref = "paper", xref = "paper",
                      xanchor = "left", yanchor = "top", showarrow = FALSE, font = list(size = 14), align = "left")}) %>%
  subplot(margin = 0.01, 
          shareX = TRUE, 
          shareY = TRUE, 
          nrows = 4, 
          titleX = F, 
          titleY = F) 

save_widget(ct_subdistrict_daily_counts_bar_chart, private_destdir)

# wc_daily_count_timeseries --------------
wc_daily_confirmed_cases <- wc_all_cases %>% 
  select(Date.of.Diagnosis) %>% 
  group_by(Date.of.Diagnosis) %>% 
  summarise(count = n()) %>% ungroup()

# wc_cumulative_count_timeseries --------------
wc_confirmed_timeseries <- wc_daily_confirmed_cases %>% 
  mutate(WC = cumsum(count)) %>% 
  rename(date = Date.of.Diagnosis) %>% 
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
    axis.text.x=element_blank())

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
  filter(Scenario == "default") %>% 
  mutate(TimeInterval = as.Date(TimeInterval))

wc_cumulative_deaths <- wc_all_cases %>% 
  select(Date.of.Death) %>% 
  drop_na() %>% 
  group_by(Date.of.Death) %>% 
  summarise(daily_deaths = n()) %>% 
  ungroup() %>% 
  arrange(Date.of.Death) %>% 
  mutate(cumulative_deaths = cumsum(daily_deaths))

wc_model_latest_default <- left_join(wc_model_latest_default, wc_cumulative_deaths, by = c("TimeInterval" = "Date.of.Death"))

# Write this back for fatalities management
write_csv(wc_model_latest_default, "data/private/wc_model_latest_default.csv")
file_to_minio("data/private/wc_model_latest_default.csv",
              "covid",
              minio_key,
              minio_secret,
              "EDGE",
              filename_prefix_override = "data/private/")

wc_model_latest_disease_figures <- plot_ly(wc_model_latest_default,
                                           x = ~TimeInterval,
                                           y = ~NewInfections, type = 'bar',
                                           name = 'Daily New Infections') %>%
  add_trace(y = ~TotalDeaths, name = 'Modeled Cumulative Deaths') %>%
  add_trace(y = ~cumulative_deaths, name = 'Actual Cumulative Deaths',
            type = "scatter", 
            mode = "line", 
            line = list(color = 'rgba(219, 64, 82, 1)'), 
            marker = list(color = 'rgba(219, 64, 82, 1)')) %>%
  layout(legend = list(orientation = 'h'), xaxis = list(title = ""), yaxis = list(title = ""))

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


wc_latest_modeled_deaths <- wc_model_latest_default %>% 
  filter(TimeInterval == as.Date(Sys.time())) %>% 
  pull(TotalDeaths) %>% .[[1]]

wc_latest_modeled_cumulative_cases <- wc_model_latest_default %>% 
  filter(TimeInterval == as.Date(Sys.time())) %>% 
  pull(TotalInfections) %>% .[[1]]


wc_latest_modeled_active_cases <- wc_model_latest_default %>% 
  arrange(TimeInterval) %>%
  filter(TimeInterval >= (as.Date(Sys.time()) - 14)) %>%
  mutate(modeled_active_cases = cumsum(NewInfections)) %>% 
  filter(TimeInterval == as.Date(Sys.time())) %>%
  pull(modeled_active_cases) %>% .[[1]]

wc_modeled_reach_100_deaths_per_day <- wc_model_latest_default %>% 
  arrange(TimeInterval) %>%
  filter(TimeInterval >= as.Date(Sys.time())) %>%
  mutate(more_than_100_deaths_per_day = if_else(NewDeaths > 100, T, F))  %>%
  filter(more_than_100_deaths_per_day == T) %>%
  pull(TimeInterval) %>% min(.[[1]])


wc_modeled_days_to_100_deaths_per_day <- wc_model_latest_default %>% filter(TimeInterval >= as.Date(Sys.time()),
                                     TimeInterval <= wc_modeled_reach_100_deaths_per_day) %>% nrow() 


latest_private_values <- append(latest_private_values,
                                listN(wc_latest_modeled_deaths,
                                      wc_latest_modeled_cumulative_cases,
                                      wc_latest_modeled_active_cases,
                                      wc_modeled_days_to_100_deaths_per_day))

write(
  toJSON(latest_private_values), 
  file.path(getwd(), private_destdir,"latest_values.json")
)

# USA COUNTY DEATHS SINCE 25 ============================================

usa_county_deaths_per_million_trajectory <-   usa_county_deaths_since_25 %>% 
  rownames_to_column(var = "day") %>% mutate(day = as.numeric(day)) %>%
  select(-days_since_passed_25) %>% gather(key = "county", value = "deaths", -day)

usa_county_deaths_per_million_trajectory <- left_join(usa_county_deaths_per_million_trajectory, 
                                                      usa_county_populations, by = c("county" = "Combined_Key")) %>% 
  filter(Population > 100000) %>%
  mutate(deaths_per_million = deaths / Population * 10^6) %>% select(day, county, deaths_per_million) %>% 
  spread(key = "county", value = "deaths_per_million") %>% arrange(day) %>% select(-day)

ct_deaths_since_25 <- ct_subdistrict_cumulative_daily_counts %>% 
  group_by(date) %>% 
  summarize(`Cape Town, WC`= sum(cumulative_deaths)) %>% 
  ungroup() %>% arrange(date) %>% filter(`Cape Town, WC` >= 25) %>%
  pull(`Cape Town, WC`) 

ct_deaths_per_million <- ct_deaths_since_25 / sum(cct_mid_year_2019_pop_est$AMOUNT) * 10^6

ct_deaths_per_million <- c(ct_deaths_per_million, rep(NA, nrow(usa_county_deaths_per_million_trajectory) - length(ct_deaths_per_million) ))

usa_county_deaths_per_million_trajectory$`Cape Town, WC` <- ct_deaths_per_million

usa_county_deaths_per_million_trajectory <- usa_county_deaths_per_million_trajectory %>%
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
  dySeries(name = "Cape Town, WC",  color = "red", label = "Cape Town, WC", strokeWidth = 5)
save_widget(usa_county_deaths_per_million_trajectory, public_destdir)

usa_county_deaths_per_million_trajectory_log <- usa_county_deaths_per_million_trajectory %>% dyOptions(logscale = TRUE)
save_widget(usa_county_deaths_per_million_trajectory_log, public_destdir)


# income data --------------------------------
# Filter out zero days in the future
# latest_income_total_date <- min(Sys.Date(), max(income_totals$DateTimestamp))
# income_totals <- income_totals %>% filter(DateTimestamp <= latest_income_total_date)
# 
# income_totals_cash <- income_totals %>%
#   gather(key = "channel", value = "amount", -DateTimestamp) %>%
#   group_by(DateTimestamp) %>%
#   summarise(daily_revenue = sum(amount)) %>%
#   ungroup() %>% mutate(year = year(DateTimestamp),
#                          month = month(DateTimestamp)) %>% group_by(year, month) %>% summarise(mean_daily_revenue = mean(daily_revenue)) %>% ungroup()
# 
# income_totals_cash$month <- factor(income_totals_cash$month)
# levels(income_totals_cash$month) <- month.abb
# 
# # plotting reference lines across each facet:
# 
# referenceLines <- income_totals_cash  # \/ Rename
# colnames(referenceLines)[2] <- "groupVar"
# zp <- ggplot(income_totals_cash,
#              aes(x = year, y = mean_daily_revenue))
# zp <- zp + geom_line(data = referenceLines,  # Plotting the "underlayer"
#                      aes(x = year, y = mean_daily_revenue, group = groupVar),
#                      colour = "GRAY", alpha = 1/2, size = 1/2)
# zp <- zp + geom_line(size = 1)  # Drawing the "overlayer"
# zp <- zp + facet_wrap(~ month)
# zp <- zp + theme_bw()
# 
# ggplotly()
# 
# # Stacked category chart
# date_window <- substr(seq(from = latest_income_total_date - 30, to = latest_income_total_date, by = "days"), start = 6, stop = 10)
# income_totals_all <- income_totals %>%
#   mutate(filter_key = substr(DateTimestamp, start = 6, stop = 10)) %>%
#   filter(filter_key %in% date_window) %>% select(-filter_key)
# 
# income_totals_all <- income_totals_all %>%
#   gather(key = "channel", value = "amount", -DateTimestamp)  %>%
#   mutate(year = year(DateTimestamp),
#                        day = yday(DateTimestamp)) %>%
#   group_by(year, channel) %>%
#   summarise(average_amount = mean(amount)) %>%
#   ungroup() %>%
#   spread(key = channel, value = average_amount)
# 
# daily_ave_income_categories <- plot_ly(income_totals_all, x = ~year, y = ~Bank, name = 'Bank', type = 'bar')
# daily_ave_income_categories <- daily_ave_income_categories %>% add_trace(y = ~Cash, name = 'Cash')
# daily_ave_income_categories <- daily_ave_income_categories %>% add_trace(y = ~DebitOrders, name = 'Debit Orders')
# daily_ave_income_categories <- daily_ave_income_categories %>% add_trace(y = ~EPortal, name = 'ePortal')
# daily_ave_income_categories <- daily_ave_income_categories %>% add_trace(y = ~GroupAccounts, name = 'Group Accounts')
# daily_ave_income_categories <- daily_ave_income_categories %>% add_trace(y = ~UnidentifiedCash, name = 'Unidentified Cash')
# daily_ave_income_categories <- daily_ave_income_categories %>% layout(title = '',
#                       xaxis = list(title = "",
#                                    showgrid = FALSE),
#                       yaxis = list(title = "Average Daily Income in the Last 30 Days (Rm)",
#                                    showgrid = FALSE),
#                       barmode = "stack")
# 
# daily_ave_income_categories


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
