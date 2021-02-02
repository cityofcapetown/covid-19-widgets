# BOILERPLATE =================================================================================================
rm(list=ls())
options(scipen=999)
args = commandArgs(trailingOnly=TRUE)

# SETENV FOR DAG COMPATIBILITY ===================================================================================
if (Sys.getenv("DB_UTILS_DIR") == "") {
  Sys.setenv("DB_UTILS_DIR" = "~/db-utils")
}
if (Sys.getenv("SECRETS_FILE") == "") {
  Sys.setenv("SECRETS_FILE" = "secrets.json")
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
library(dplyr) # extra dependency for lag day adjustments data manipulation
library(R.utils)
library(webshot2)
library(chromote)


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

save_widget <- function(widg, destdir, name_override=NULL, screenshot = TRUE) {
  widget_name <- deparse(substitute(widg))
  if (!is.null(name_override)){
    widget_name <- name_override
    print(paste("Overriding widget name to ", widget_name))
  }
  
  savepath <- file.path(getwd(), destdir, 
                        paste(widget_name, "html", sep = "."))
  libdir <- file.path(getwd(), destdir, 
                      "libdir")
  r_savepath <- file.path(getwd(), destdir, 
                          paste(widget_name, "rds", sep = "."))
  
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
    if(isTRUE(screenshot)) {
      png_savepath <- file.path(getwd(), destdir, 
                                paste(widget_name, "png", sep = "."))
      
      withTimeout({
        webshot2::webshot(savepath, file = png_savepath)
      }, timeout = 10, onTimeout = "warning")
      
    }
    print(paste("Saved to", savepath))
  }
  saveRDS(widg, r_savepath)
}


# INITIALISE CHROME BROWSER ==================================================
# Need to do this for running chromium in a docker container
set_default_chromote_object(
  Chromote$new(browser = Chrome$new(args = "--no-sandbox"))
)

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

# Latest values Subdistricts
subdistrict_latest_df <- ct_all_cases %>% 
  group_by(Subdistrict) %>% 
  summarise(latest_deaths = sum(!is.na(Date.of.Death)),
            latest_confirmed = sum(!is.na(Date.of.Diagnosis))) %>%
  ungroup() %>%
  mutate(Subdistrict = tolower(Subdistrict)) %>%
  gather(key = "metric", value = "count", -Subdistrict) %>%
  mutate(metric = paste(Subdistrict, metric)) %>% select(-Subdistrict) %>%
  mutate(metric = str_replace_all(metric, " ", "_"))

subdistrict_latest <- as.list(subdistrict_latest_df$count)
names(subdistrict_latest) <- subdistrict_latest_df$metric

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
  filter(age >= 0) %>%
  mutate(age_interval = findInterval(age, age_brackets, rightmost.closed = TRUE)) %>%
  drop_na(age_interval) %>%
  mutate(age_interval = age_bracket_labels[age_interval]) %>%
  group_by(age_interval) %>%
  summarise(ct_raw_age_confirmed_cases = n(), .groups = "keep") %>%
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
                       global_last_deaths_val,
                       subdistrict_latest)



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
spread_ct_subdistrict_daily_confirmed_cases <-  ct_subdistrict_daily_confirmed_cases %>% spread(key = Subdistrict, value = count) %>% drop_na(date)
spread_ct_subdistrict_daily_confirmed_cases[is.na(spread_ct_subdistrict_daily_confirmed_cases)] <- 0
ct_subdistrict_daily_confirmed_cases <- spread_ct_subdistrict_daily_confirmed_cases %>% gather(key = "Subdistrict", value = count, -date)

every_nth = function(n) {
  return(function(x) {x[c(TRUE, rep(FALSE, n - 1))]})
}

# ct subdistrict presumed active
ct_subdistrict_daily_active_cases  <- 
  ct_all_cases_parsed %>% 
  rename(date = Date.of.Diagnosis) %>%
  drop_na(date) %>%
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
  p <- p %>% layout(yaxis = list(title = 'Count'), barmode = 'stack', legend = list(x = 0.1, y = 0.9),
                    margin=list(l=5, r=5, b=5, t=5, pad=5))
  obj_name <- print(paste("cct_", str_replace_all(str_replace_all(subdist, " ", "_"), "&", ""), "_cumulative_counts", sep = ""))  
  assign(obj_name, p)
  savepath <- file.path(getwd(), private_destdir, 
                        paste(obj_name, "html", sep = "."))
  libdir <- file.path(getwd(), private_destdir, 
                      "libdir")
  save_widget(p, private_destdir, obj_name)
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


#---------------------------------
# Add lag - day adjustment curve to the ct daily counts bar chart
#---------------------------------

# Get the latest spv export date
ct_latest_export_date <- ct_all_cases_parsed$Export.Date[nrow(ct_all_cases_parsed)-1]
# add lag day to ct_daily_counts
ct_daily_counts$lag_days <- as.Date(mdy_hm(ct_latest_export_date)) - as.Date(as.character(ct_daily_counts$date), format="%Y-%m-%d")
ct_daily_counts$lag_days <-  as.numeric(ct_daily_counts$lag_days, units="days")

# filter to diagnosis lag and merge the diag median on lag_days
DiagnosisDate_freq_table <- spv_lag_freq_table_wc_districts %>% filter(lag_type == "Date.of.Diagnosis" & District == "City of Cape Town")
DiagnosisDate_freq_table <- DiagnosisDate_freq_table %>% select(lag_days, median)
ct_daily_counts <- ct_daily_counts %>% left_join(DiagnosisDate_freq_table, by="lag_days")
# fillna with 1 for zero adjustment
ct_daily_counts$median[is.na(ct_daily_counts$median)] <- 1
# set cases lag day 1 adjustment to not adjust due to extreme instability
ct_daily_counts$median[ct_daily_counts$lag_days == 1] <- NA
# caclulate the adjusted values
ct_daily_counts$cases_adjusted <- ct_daily_counts$cases / ct_daily_counts$median
ct_daily_counts <- ct_daily_counts %>% select(-median)
ct_daily_counts$cases_adjusted <- round(ct_daily_counts$cases_adjusted, 0)

# filter to death lag and merge the deaths median on lag_days
DeathDate_freq_table <- spv_lag_freq_table_wc_districts %>% filter(lag_type == "Date.of.Death" & District == "City of Cape Town")
DeathDate_freq_table <- DeathDate_freq_table %>% select(lag_days, median)
ct_daily_counts <- ct_daily_counts %>% left_join(DeathDate_freq_table, by="lag_days")
# fillna with 1 for zero adjustment
ct_daily_counts$median[is.na(ct_daily_counts$median)] <- 1
# set cases lag day 1 adjustment to not adjust due to extreme instability
ct_daily_counts$median[ct_daily_counts$lag_days == 1] <- NA
# caclulate the adjusted values
ct_daily_counts$Deaths_adjusted <- ct_daily_counts$deaths / ct_daily_counts$median
ct_daily_counts <- ct_daily_counts %>% select(-median)
ct_daily_counts$Deaths_adjusted <- round(ct_daily_counts$Deaths_adjusted, 0)
# get the  5 day rolling cases and deaths
ct_daily_counts <-  ct_daily_counts %>% 
  mutate(rolling_death_5_days_adjust = rollmean(ct_daily_counts$Deaths_adjusted, 5, na.pad=TRUE, align="right"),
         rolling_cases_5_days_adjust = rollmean(ct_daily_counts$cases_adjusted, 5, na.pad=TRUE, align="right"))

#---------------------------------
#---------------------------------

ct_rolling_5_day_deaths_latest <- ct_daily_counts$rolling_death_5_days[nrow(ct_daily_counts)]
ct_rolling_5_day_cases_latest <- ct_daily_counts$rolling_cases_5_days[nrow(ct_daily_counts)]
ct_rolling_5_day_deaths_yesterday <- ct_daily_counts$rolling_death_5_days[nrow(ct_daily_counts)-1]
ct_rolling_5_day_cases_yesterday <- ct_daily_counts$rolling_cases_5_days[nrow(ct_daily_counts)-1]
ct_rolling_5_day_deaths_day_change <- ct_rolling_5_day_deaths_latest - ct_rolling_5_day_deaths_yesterday
ct_rolling_5_day_cases_day_change <- ct_rolling_5_day_cases_latest - ct_rolling_5_day_cases_yesterday

latest_values <- append(latest_values,
                        listN(ct_rolling_5_day_deaths_latest,
                              ct_rolling_5_day_cases_latest,
                              ct_rolling_5_day_deaths_yesterday,
                              ct_rolling_5_day_cases_yesterday,
                              ct_rolling_5_day_deaths_day_change,
                              ct_rolling_5_day_cases_day_change))

write(
  toJSON(latest_values), 
  file.path(getwd(), public_destdir,"latest_values.json")
)

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
  add_trace(y = ~rolling_death_5_days_adjust, name = 'Deaths 5 Day Average <br> * adjusted for reporting lag',
            type = "scatter", 
            mode = "line", 
            line = list(color = 'rgba(219, 64, 82, 1)'), 
            marker = list(color = 'rgba(219, 64, 82, 1)')) %>%
  add_trace(y = ~rolling_cases_5_days_adjust, name = 'New Cases 5 Day Average  <br> * adjusted for reporting lag',
            type = "scatter", 
            mode = "line", 
            line = list(color = 'rgba(55, 128, 191, 1)'), 
            marker = list(color = 'rgba(55, 128, 191, 1)')) %>%
  layout(barmode = 'stack', legend = list(x = 0.1, y = 0.9))

save_widget(ct_daily_counts_bar_chart, private_destdir)

#---------------------------------
# Add the lag adjustments for the subdistricts plot
#---------------------------------

# add lag day
ct_subdistrict_cumulative_daily_counts$lag_days <- as.Date(mdy_hm(ct_latest_export_date)) - as.Date(as.character(ct_subdistrict_cumulative_daily_counts$date), format="%Y-%m-%d")
ct_subdistrict_cumulative_daily_counts$lag_days <- as.numeric(ct_subdistrict_cumulative_daily_counts$lag_days, units="days")

# get the cases lag_days by subdistrict
Dist_DiagnosisDate_freq_table <- spv_lag_freq_table_wc_subdistricts %>% filter(lag_type == "Date.of.Diagnosis" & District == "City of Cape Town")
Dist_DiagnosisDate_freq_table <- Dist_DiagnosisDate_freq_table %>% select(lag_days, median, Subdistrict)
# merge the diagnosed cases lag by subdistrict
ct_subdistrict_cumulative_daily_counts <- left_join(ct_subdistrict_cumulative_daily_counts, Dist_DiagnosisDate_freq_table, by=c("lag_days", "Subdistrict"))
# fillna with 1 for zero adjustment
ct_subdistrict_cumulative_daily_counts$median[is.na(ct_subdistrict_cumulative_daily_counts$median)] <- 1
# set cases lag day 1 adjustment to not adjust due to extreme instability
ct_subdistrict_cumulative_daily_counts$median[ct_subdistrict_cumulative_daily_counts$lag_days == 1] <- NA
# caclulate the adjusted values
ct_subdistrict_cumulative_daily_counts$cases_adjusted <- ct_subdistrict_cumulative_daily_counts$cases / ct_subdistrict_cumulative_daily_counts$median
ct_subdistrict_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% select(-median)
# round adjusted cases to int
ct_subdistrict_cumulative_daily_counts$cases_adjusted <- round(ct_subdistrict_cumulative_daily_counts$cases_adjusted, 0)

# get the deaths lag_days by subdistrict
Dist_DeathDate_freq_table <- spv_lag_freq_table_wc_subdistricts %>% filter(lag_type == "Date.of.Death" & District == "City of Cape Town")
Dist_DeathDate_freq_table <- Dist_DeathDate_freq_table %>% select(lag_days, median, Subdistrict)
# merge the deaths lag by subdistrict
ct_subdistrict_cumulative_daily_counts <- left_join(ct_subdistrict_cumulative_daily_counts, Dist_DeathDate_freq_table, by=c("lag_days", "Subdistrict"))
# fillna with 1 for zero adjustment
ct_subdistrict_cumulative_daily_counts$median[is.na(ct_subdistrict_cumulative_daily_counts$median)] <- 1
# set cases lag day 1 adjustment to not adjust due to extreme instability
ct_subdistrict_cumulative_daily_counts$median[ct_subdistrict_cumulative_daily_counts$lag_days == 1] <- NA
# caclulate the adjusted deaths values
ct_subdistrict_cumulative_daily_counts$deaths_adjusted <- ct_subdistrict_cumulative_daily_counts$deaths / ct_subdistrict_cumulative_daily_counts$median
# also remove the lag days column since it isn't needed anymore
ct_subdistrict_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% select(-median, -lag_days)
# round adjusted cases to int
ct_subdistrict_cumulative_daily_counts$deaths_adjusted <- round(ct_subdistrict_cumulative_daily_counts$deaths_adjusted, 0)
ct_subdistrict_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts[order(ct_subdistrict_cumulative_daily_counts$date),]
#---------------------------------
#---------------------------------


for (subdist in unique(ct_subdistrict_cumulative_daily_counts$Subdistrict)) {
  subdist_cumulative_daily_counts <- ct_subdistrict_cumulative_daily_counts %>% 
    filter(Subdistrict == subdist) %>%  
    mutate(rolling_death_5_days = rollmean(deaths_adjusted, 5, na.pad=TRUE, align="right"),
           rolling_cases_5_days = rollmean(cases_adjusted, 5, na.pad=TRUE, align="right"))
  
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
    add_trace(y = ~rolling_death_5_days, name = 'Deaths 5 Day Average <br> * adjusted for reporting lag',
              type = "scatter", 
              mode = "line", 
              line = list(color = 'rgba(219, 64, 82, 1)'), 
              marker = list(color = 'rgba(219, 64, 82, 1)')) %>%
    add_trace(y = ~rolling_cases_5_days, name = 'New Cases 5 Day Average  <br> * adjusted for reporting lag',
              type = "scatter", 
              mode = "line", 
              line = list(color = 'rgba(55, 128, 191, 1)'), 
              marker = list(color = 'rgba(55, 128, 191, 1)')) %>%
    
    layout(barmode = 'stack', legend = list(x = 0.1, y = 0.9), margin=list(l=5, r=5, b=5, t=5, pad=5))
  obj_name <- print(paste("cct_", str_replace_all(str_replace_all(subdist, " ", "_"), "&", ""), "_daily_counts_bar_chart", sep = ""))  
  assign(obj_name, p)
  savepath <- file.path(getwd(), private_destdir, 
                        paste(obj_name, "html", sep = "."))
  libdir <- file.path(getwd(), private_destdir, 
                      "libdir")
  save_widget(p, private_destdir, obj_name)
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
save_widget(wc_confirmed_timeseries, private_destdir,  screenshot = FALSE)

wc_confirmed_timeseries_log <- wc_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(wc_confirmed_timeseries_log, private_destdir,  screenshot = FALSE)

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
  filter(TimeInterval == min(as.Date(Sys.time()), max(wc_model_latest_default$TimeInterval))) %>%
  pull(TotalDeaths) %>% .[[1]]

wc_latest_modeled_cumulative_cases <- wc_model_latest_default %>%
  filter(TimeInterval == min(as.Date(Sys.time()), max(wc_model_latest_default$TimeInterval))) %>%
  pull(TotalInfections) %>% .[[1]]

wc_latest_modeled_active_cases <- wc_model_latest_default %>%
  arrange(TimeInterval) %>%
  filter(TimeInterval >= (min(as.Date(Sys.time()), max(wc_model_latest_default$TimeInterval)) - 14)) %>%
  mutate(modeled_active_cases = cumsum(NewInfections)) %>%
  filter(TimeInterval == min(as.Date(Sys.time()), max(wc_model_latest_default$TimeInterval))) %>%
  pull(modeled_active_cases) %>% .[[1]]

latest_private_values <- append(latest_private_values,
                                listN(wc_latest_modeled_deaths,
                                      wc_latest_modeled_cumulative_cases,
                                      wc_latest_modeled_active_cases)
)

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
