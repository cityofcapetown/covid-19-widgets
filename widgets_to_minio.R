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
  select(-YYYYMMDD) %>% 
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
# TODO Replace with WC direct data
wc_latest_update <- rsa_latest_update
wc_latest_confirmed <- max(rsa_provincial_ts_confirmed$WC)

# expected_future_trajectory_log -----------------------
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

# Age brackets
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

# China https://www.ncbi.nlm.nih.gov/pubmed/32064853?fbclid=IwAR3JCxH50VTfg3Q_02YTLdz2Tk7yBTmt-5oCxE4KlBe0evh7ByK3HPVU-pU
# https://ourworldindata.org/uploads/2020/03/Coronavirus-CFR-by-age-in-China-1.png
chinese_age_fatality_rate <- c(0, 0.2, 0.2, 0.2, 0.4, 1.3, 3.6, 8, 14.8)

# China demographic
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
  mutate(fatality_rate_pct = chinese_age_fatality_rate) %>%
  mutate(population = "China Pop %",
         fatal_label = "China Fatality Rate %") 

# RSA
# TODO - LINK TO DATA WHEN IT BECOMES AVAILABLE
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
  mutate(fatality_rate_pct = rsa_age_fatality_rate) %>% 
  mutate(population = "SA Pop %",
         fatal_label = "SA Fatality Rate %") 

# CAPE TOWN
# TODO - LINK TO DATA WHEN IT BECOMES AVAILABLE
cct_age_fatality_rate <- c(0, 0, 0, 0, 0, 0, 0, 0, 0)

cct_demographic <- cct_mid_year_2019_pop_est %>% 
  mutate(age_interval = NORM_AGE_COHORT) %>%
  group_by(age_interval) %>% 
  summarise(cct_population = sum(AMOUNT)) %>% 
  ungroup() %>%
  mutate(population_pct = cct_population/sum(cct_population)*100) %>%
  select(-cct_population) %>%
  mutate(fatality_rate_pct = rsa_age_fatality_rate) %>% 
  mutate(population = "CCT Pop %",
         fatal_label = "CCT Fatality Rate %") 


# RSA total confirmed
rsa_total_confirmed <- rsa_provincial_ts_confirmed %>% select(-YYYYMMDD) %>% 
  rowSums(.) %>% 
  enframe(., value = "rsa_confirmed") %>% 
  mutate(report_date = as_date(rsa_provincial_ts_confirmed$YYYYMMDD)) %>% 
  select(-name)

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
latest_values <- listN(wc_latest_update,
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
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>%
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.4,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE) %>%
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
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE,
            logscale = FALSE) 
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
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE) 

save_widget(rsa_transmission_type_timeseries, public_destdir)

rsa_transmission_type_timeseries_log <- rsa_transmission_type_timeseries %>% dyOptions(logscale = TRUE)
save_widget(rsa_transmission_type_timeseries_log, public_destdir)


# rsa_provincial_timeseries --------------
rsa_provincial_confirmed_timeseries <- rsa_provincial_ts_confirmed %>% 
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dySeries(name = "WC", label = "WC", color = "red", strokeWidth = 5) %>%
  dyOptions(stackedGraph = TRUE) 
save_widget(rsa_provincial_confirmed_timeseries, public_destdir)

rsa_provincial_confirmed_timeseries_log <- rsa_provincial_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(rsa_provincial_confirmed_timeseries_log, public_destdir)


# wc_timeseries --------------
wc_confirmed_timeseries <- rsa_provincial_ts_confirmed %>% 
  select(YYYYMMDD, WC) %>%
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = TRUE) 
save_widget(wc_confirmed_timeseries, public_destdir)

wc_confirmed_timeseries_log <- wc_confirmed_timeseries %>% dyOptions(logscale = TRUE)
save_widget(wc_confirmed_timeseries_log, public_destdir)


# rsa_timeline_testing ----------------------------------
rsa_timeline_testing <- covid19za_timeline_testing %>% 
  select(YYYYMMDD, cumulative_tests) %>%
  df_as_xts("YYYYMMDD") %>% 
  dygraph() %>%
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.5,
              hideOnMouseOut = TRUE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = FALSE,
            logscale = FALSE) 
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
  china_demographic %>% mutate(fatality_rate_pct = -fatality_rate_pct,
                                population_pct = -population_pct) %>%
    rbind(., rsa_demographic) %>%
    ggplot(aes(x = age_interval, y = population_pct)) +
    geom_bar(aes(fill = population), 
             alpha = 6/10,
             stat = "identity") +
    geom_bar(aes(y = fatality_rate_pct, fill = fatal_label), 
             
             width = 0.5, 
             stat = "identity", group = 1) +
    
    scale_fill_manual(values = c(`SA Pop %` = "#D55E00", 
                                 `China Pop %` = "#E69F00", 
                                 `SA Fatality Rate %` = "#D55E00",
                                 `China Fatality Rate %` = "#E69F00"), 
                      name="") +
    coord_flip() +
    labs(x = "", y = "China vs RSA Age Demographics and COVID Case Fatality Rate (%)") +
    theme_bw()
  
rsa_demographic_mortality_plot <- ggplotly(rsa_demographic_mortality_plot)  %>% plotly::config(displayModeBar = F)  
save_widget(rsa_demographic_mortality_plot, public_destdir)

# cape town demographic mortality plot ---------------------
cct_demographic_mortality_plot <- 
  china_demographic %>% mutate(fatality_rate_pct = -fatality_rate_pct,
                               population_pct = -population_pct) %>%
  rbind(., cct_demographic) %>%
  ggplot(aes(x = age_interval, y = population_pct)) +
  geom_bar(aes(fill = population), 
           alpha = 6/10,
           stat = "identity") +
  geom_bar(aes(y = fatality_rate_pct, fill = fatal_label), 
           
           width = 0.5, 
           stat = "identity", group = 1) +
  
  scale_fill_manual(values = c(`CCT Pop %` = "#D55E00", 
                               `China Pop %` = "#E69F00", 
                               `CCT Fatality Rate %` = "#D55E00",
                               `China Fatality Rate %` = "#E69F00"), 
                    name="") +
  coord_flip() +
  labs(x = "", y = "China vs CCT Age Demographics and COVID Case Fatality Rate (%)") +
  theme_bw()

cct_demographic_mortality_plot <- ggplotly(cct_demographic_mortality_plot)  %>% plotly::config(displayModeBar = F)  
save_widget(cct_demographic_mortality_plot, public_destdir)

# china demographic mortality plot ---------------------
china_demographic_mortality_plot <- 
  china_demographic %>% mutate(fatality_rate_pct = -fatality_rate_pct,
                               population_pct = -population_pct) %>%
  ggplot(aes(x = age_interval, y = population_pct)) +
  geom_bar(aes(fill = population), 
           alpha = 6/10,
           stat = "identity") +
  geom_bar(aes(y = fatality_rate_pct, fill = fatal_label), 
           width = 0.5, 
           stat = "identity", group = 1) +
  scale_fill_manual(values = c(`SA Pop %` = "#D55E00", 
                               `China Pop %` = "#E69F00", 
                               `SA Fatality Rate %` = "#D55E00",
                               `China Fatality Rate %` = "#E69F00"), 
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
  #dyLegend(width = 400) %>%
  dyCSS(textConnection("
     .dygraph-legend > span { display: none; }
     .dygraph-legend > span.highlight { display: inline; }
  ")) %>% 
  dyHighlight(highlightCircleSize = 5, 
              highlightSeriesBackgroundAlpha = 0.2,
              hideOnMouseOut = FALSE) %>%
  dyRangeSelector(height = 20) %>%
  dyOptions(stackedGraph = TRUE, strokeWidth = c(1,5) )

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
         mortality_per_1m,case_fatality_rate_pct) %>%
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

# SEND TO MINIO =================================================================
for (filename in list.files(public_destdir)) {
  filepath <- file.path(public_destdir, filename)
  if (file_ext(filepath) != "") {
    print(filepath)
    file_to_minio(file.path(public_destdir, filename),
                  "covid",
                  minio_key,
                  minio_secret,
                  "EDGE",
                  filename_prefix_override = paste(public_destdir, "/", sep=""))
    print("Sent")
  } else {
    print(filepath)
    print("This is a directory - not sending!")
    }
}

# Save a copy of the data to .Rdata for dashboard knit
save.image(file = "widgets.RData")
