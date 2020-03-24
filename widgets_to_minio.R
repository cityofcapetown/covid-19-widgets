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

save_widget <- function(widg) {
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

load_rgdb_table <- function(table_name, minio_key, minio_secret) {
  temp_dir <- tempdir()
  cat("Loading", table_name, "from rgdb bucket and reading into memory \n")
  filename = file.path(temp_dir, paste(table_name, ".parquet", sep = ""))
  if (!file.exists(filename)) {
    minio_to_file(filename,
                  minio_key=minio_key,
                  minio_secret=minio_secret,
                  minio_bucket="rgdb",
                  data_classification= "EDGE")
  }
  wkt_df <- read_parquet(filename)
  cat("Converting", table_name, "data frame into spatial feature \n")
  geo_layer <- st_as_sf(wkt_df, wkt = "EWKT")
  names(st_geometry(geo_layer)) <- NULL
  return(geo_layer)
}

# CREATE DIRS =================================================================
sourcedir <- "data/public"
dir.create(sourcedir, recursive = TRUE)

destdir <- "widgets/public"
dir.create(destdir, recursive = TRUE)

# PULL IN PUBLIC DATA =======================================================
covid_assets <- bucket_objects_to_df("covid", 
                     minio_key,
                     minio_secret,
                     "ds2.capetown.gov.za")

public_datasets <- covid_assets %>% 
  filter(grepl("data/public",object_name) ) %>% 
  filter(grepl(".csv",object_name)) %>%
  pull(object_name) %>% as.character() 

for (object_name in public_datasets) {
  minio_to_file(object_name,
                "covid",
                minio_key,
                minio_secret,
                "EDGE",
                minio_filename_override = object_name)
}

public_dataset_names <- strsplit(public_datasets, "\\/")
public_dataset_names <- sapply(public_dataset_names, "[[", 3)
public_dataset_names <- strsplit(public_dataset_names, "\\.")
public_dataset_names <- sapply(public_dataset_names, "[[", 1) 

# Load all
for (i in seq_along(1:length(public_datasets))) {
  df <- read_csv(public_datasets[i])
  assign(paste(public_dataset_names[i]), df)
}
rm(df)

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
rsa_latest_update <- max(sa_ts_confirmed$YYYYMMDD)

rsa_latest_confirmed <- max(sa_ts_confirmed$confirmed)
rsa_latest_deaths <- nrow(covid19za_timeline_deaths)
rsa_latest_tested <- max(covid19za_timeline_testing$cumulative_tests)

# Latest Values WC --------------------------
wc_latest_update <- rsa_latest_update
wc_latest_confirmed <- max(rsa_provincial_ts_confirmed$WC)

# expected_future_trajectory_log -----------------------
countries_this_far <- global_ts_since_100 %>% 
  select(-days_since_passed_100) %>% 
  apply(., MARGIN = 1, function(x) sum(!is.na(x)))

median_values <- global_ts_since_100 %>% select(-days_since_passed_100) %>%
  t() %>% as_data_frame() %>%
  summarise_all(funs(median), na.rm = T, funs(quantile), probs = 0.25) %>%
  t() 

lower_quartile_values <- global_ts_since_100 %>% select(-days_since_passed_100) %>%
  t() %>% as_data_frame() %>%
  summarise_all(funs(quantile), probs = 0.25, na.rm = T) %>%
  t() 

upper_quartile_values <- global_ts_since_100 %>% select(-days_since_passed_100) %>%
  t() %>% as_data_frame() %>%
  summarise_all(funs(quantile), probs = 0.85, na.rm = T) %>%
  t() 

values_to_drop <- ifelse(countries_this_far < 8, NA, 1)

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

# PULL IN AND PREPARE GEO DATA ==========================================
# Pull wards spatial layer
wards <- load_rgdb_table("LDR.SL_CGIS_WARD", minio_key, minio_secret)
wards_2016 <- wards %>% filter(WARD_YEAR == 2016)

wards_2016_polygons <- wards_2016 %>% select(WARD_NAME)

wards_2016_density <- read_csv("data/public/ward_density_2016.csv") %>% 
  mutate(WARD_NAME = as.character(WARD)) %>%
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2`) 

cct_2016_pop_density <- left_join(wards_2016_polygons, wards_2016_density, by = "WARD_NAME") %>% 
  select(WARD_NAME, `2016_POP`, `2016_POP_DENSITY_KM2` ) 

# Pull health care regions
health_districts <- load_rgdb_table("LDR.SL_CGIS_CITY_HLTH_RGN", minio_key, minio_secret)

# Pull health car facilities
health_care_facilities <- load_rgdb_table("LDR.SL_ENVH_HLTH_CARE_FCLT", minio_key, minio_secret)


informal_taps <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TAPS", minio_key, minio_secret)
informal_toilets <- load_rgdb_table("LDR.SL_WTSN_IS_UPDT_TLTS", minio_key, minio_secret)
informal_settlements <- load_rgdb_table("LDR.SL_INF_STLM", minio_key, minio_secret)

#informal_settlements %>% st_geometry() %>% mapview::mapview()

#write_sf(health_districts, "health_districts.geojson", quiet = TRUE, append = FALSE, delete_layer = TRUE)
#write_sf(wards_2016, "wards_2016.geojson", quiet = TRUE, append = FALSE, delete_layer = TRUE)

# Enrich with other spatial data

# Add COVID data
# TODO ADD REAL WARD LEVEL COVID STATS
cct_wards_covid_stats <- cct_2016_pop_density %>% mutate(fake_confirmed = rgamma(nrow(wards_2016_polygons), shape = 0.5, rate = 1),
                                                        fake_deaths = rgamma(nrow(wards_2016_polygons), shape = 0.2, rate = 1))

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
  file.path(getwd(), destdir,"latest_values.json")
  )

# HTML WIDGETS ============================================================

# Expected future trajectory
future_trajectory <- 
  global_ts_since_100 %>% 
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
save_widget(future_trajectory)

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
save_widget(rsa_tests_vs_cases)

# rsa_tasts_vs_cases_rebased -------------
rsa_tests_vs_cases_rebased <- rsa_tests_vs_cases %>% dyRebase(value = 100)
save_widget(rsa_tests_vs_cases_rebased)

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

save_widget(rsa_transmission_type_timeseries)

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
save_widget(rsa_provincial_confirmed_timeseries)

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
save_widget(wc_confirmed_timeseries)

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
save_widget(rsa_timeline_testing)

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
save_widget(rsa_dem_pyramid)

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
save_widget(rsa_demographic_mortality_plot)

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
save_widget(cct_demographic_mortality_plot)

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
save_widget(china_demographic_mortality_plot)

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

save_widget(global_timeline_confirmed)

# browsable_global ------------------------------------
browsable_global <- global_latest_data %>% 
  select(country,
         population,
         confirmed,
         deaths,
         incidence_per_1m,
         mortality_per_1m,case_fatality_rate_pct) %>%
  DT::datatable(options = list(pageLength = 25))
save_widget(browsable_global)

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

save_widget(global_mortality_boxplot)

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
save_widget(global_ranked_fatalities_per_1m)

# MAPS =========================================================================

# ct_heatmap ------------------------
bins <- c(0, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, Inf)
pal <- colorBin("Blues", domain = cct_2016_pop_density$`2016_POP_DENSITY_KM2`, bins = bins)

labels <- sprintf(
  "<strong>%s</strong> <br/>%g people / km<sup>2</sup> <br/>%g residents",
  paste("Ward", cct_2016_pop_density$WARD_NAME), 
  cct_2016_pop_density$`2016_POP_DENSITY_KM2`, 
  cct_2016_pop_density$`2016_POP`) %>% 
  lapply(htmltools::HTML)

ct_heatmap <- leaflet(cct_2016_pop_density) %>%  
  addProviderTiles("Stamen.TonerLite", options = tileOptions(opacity=0.4), group="Map") %>%
  addProviderTiles('Esri.WorldImagery', options = tileOptions(opacity=0.4), group="Satellite") %>% 
  setView(lat = -33.9249, lng = 18.4241, zoom = 10L) %>%
  # ward polys
  addPolygons(fillColor = ~pal(`2016_POP_DENSITY_KM2`),
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
            values = ~`2016_POP_DENSITY_KM2`, opacity = 0.7, 
            position = "bottomright") %>%
  # control
  addLayersControl(
    baseGroups = c("Map",
                   "Satellite"),
    options = layersControlOptions(collapsed = FALSE)) 

save_widget(ct_heatmap)

# SEND TO MINIO =================================================================
for (filename in list.files(destdir)) {
  filepath <- file.path(destdir, filename)
  if (file_ext(filepath) != "") {
    print(filepath)
    file_to_minio(file.path(destdir, filename),
                  "covid",
                  minio_key,
                  minio_secret,
                  "EDGE",
                  filename_prefix_override = paste(destdir, "/", sep=""))
    print("Sent")
  } else {
    print(filepath)
    print("This is a directory - not sending!")
    }
}

# Save a copy of the data to .Rdata for dashboard knit
save.image(file = "widgets.RData")
