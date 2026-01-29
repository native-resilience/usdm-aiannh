# update.packages(repos = "https://cran.rstudio.com/",
#                 ask = FALSE)

install.packages("pak",
                 repos = "https://cran.rstudio.com/")

# installed.packages() |>
#   rownames() |>
#   pak::pkg_install(upgrade = TRUE,
#                  ask = FALSE)

pak::pak(
  c(
    "arrow",
    "sf",
    "curl",
    "tidyverse",
    "tigris",
    "rmapshaper",
    "furrr",
    "future.mirai"
  )
)

library(magrittr)
library(tidyverse)
library(sf)
library(arrow)
library(furrr)
library(future.mirai)

sf::sf_use_s2(TRUE)

dir.create(
  file.path("data","census"),
  recursive = TRUE,
  showWarnings = FALSE
)

dir.create(
  file.path("data","usdm-aiannh"),
  recursive = TRUE,
  showWarnings = FALSE
)

states <- 
  tigris::states(cb = TRUE) %>%
  sf::st_drop_geometry() %>%
  dplyr::select(STATEFP, State = NAME) %>%
  dplyr::arrange(STATEFP)

if(
  !file.exists(
    file.path("census-aiannh-2024.parquet")
  )
){
  "https://www2.census.gov/geo/tiger/TIGER2024/AIANNH/tl_2024_us_aiannh.zip" %>%
    curl::multi_download(urls = .,
                         destfiles = 
                           file.path("data","census",basename(.)),
                         resume = TRUE) %$%
    destfile %>%
    file.path("/vsizip", .) %>%
    sf::read_sf() %>%
    dplyr::select(
      GNIS = AIANNHNS,
      Name = NAME,
      `NameLSAD` = NAMELSAD,
      LSAD
    ) %>%
    dplyr::mutate(
      Name = iconv(Name, from = "latin1", to = "UTF-8"),
      `NameLSAD` = iconv(`NameLSAD`, from = "latin1", to  = "UTF-8")
    ) %>%
    sf::st_cast("MULTIPOLYGON") %>%
    sf::st_cast("POLYGON", warn = FALSE, do_split = TRUE) %>%
    sf::st_make_valid() %T>%
    {suppressMessages(sf::sf_use_s2(FALSE))} %>%
    sf::st_make_valid() %T>%
    {suppressMessages(sf::sf_use_s2(TRUE))} %>%
    # Group by class and generate multipolygons
    dplyr::group_by(GNIS, Name, NameLSAD, LSAD) %>%
    dplyr::summarise(.groups = "drop",
                     is_coverage = TRUE) %>%
    sf::st_cast("MULTIPOLYGON", warn = FALSE) %>%
    sf::st_transform("EPSG:4326") %>%
    dplyr::mutate(Area = sf::st_area(geometry)) %>%
    dplyr::select(GNIS, Name, NameLSAD, LSAD, Area) %>%
    sf::write_sf(
      file.path("census-aiannh-2024.parquet"),
      driver = "Parquet",
      layer_options = c("COMPRESSION=ZSTD",
                        "GEOMETRY_ENCODING=GEOARROW",
                        "WRITE_COVERING_BBOX=NO"),
    )
}

aiannh <-
  sf::read_sf(
    file.path("census-aiannh-2024.parquet"),
    optional = TRUE
  )

usdm_get_dates <-
  function(as_of = lubridate::today()){
    as_of %<>%
      lubridate::as_date()
    
    usdm_dates <-
      seq(lubridate::as_date("20000104"), lubridate::today(), "1 week")
    
    usdm_dates <- usdm_dates[(as_of - usdm_dates) >= 2]
    
    return(usdm_dates)
  }

plan(mirai_multisession)

usdm_get_dates() %>%
  tibble::tibble(Date = .) %>%
  dplyr::mutate(
    Year = lubridate::year(Date),
    USDM = 
      file.path(
        "https://sustainable-fsa.github.io/usdm",
        # "../usdm",
        "usdm", "data", "parquet", 
        paste0("USDM_",Date,".parquet")),
    outfile = file.path("data", "usdm-aiannh", 
                        paste0("USDM_",Date,".parquet"))
  ) %>%
  dplyr::filter(!file.exists(outfile)) %>%
  furrr::future_pwalk(
    .f = function(USDM,
                  outfile, 
                  ...){
      
      cat(USDM)
      
      if(!file.exists(outfile)){
        aiannh <-
          aiannh %>%
          sf::`st_agr<-`("constant")
        
        usdm <-
          USDM %>%
          sf::read_sf() %>%
          sf::st_transform(sf::st_crs(aiannh)) %>%
          sf::`st_agr<-`("constant")
        
        dplyr::bind_rows(
          sf::st_intersection(
            aiannh,
            usdm
          ),
          sf::st_difference(
            aiannh,
            usdm %>%
              sf::st_union()
          )
        ) %>%
          tidyr::fill(date) %>%
          sf::st_cast("MULTIPOLYGON") %>%
          sf::st_make_valid() %>%
          dplyr::arrange(Name, LSAD, date, usdm_class) %>%
          dplyr::mutate(
            usdm_date = date,
            usdm_class = 
              tidyr::replace_na(usdm_class, "None") %>%
              factor(levels = c("None", paste0("D", 0:4)),
                     ordered = TRUE),
            usdm_percent = units::drop_units(sf::st_area(geometry) / Area)
          ) %>%
          dplyr::select(GNIS, Name, NameLSAD, LSAD, 
                        usdm_date, usdm_class, usdm_percent) %>%
          dplyr::arrange(Name, LSAD, usdm_class) %>%
          sf::st_drop_geometry() %>%
          arrow::write_parquet(sink = outfile,
                               version = "latest",
                               compression = "zstd",
                               use_dictionary = TRUE)
      }
    }
  )

plan(sequential)

list.files("data/usdm-aiannh",
           recursive = TRUE,
           full.names = TRUE) %>%
  purrr::map_dfr(arrow::read_parquet) %>%
  dplyr::arrange(Name, LSAD, usdm_date, usdm_class) %>%
  arrow::write_parquet(sink = "usdm-aiannh.parquet",
                       version = "latest",
                       compression = "zstd",
                       use_dictionary = TRUE)

## Create directory listing infrastructure
generate_tree_flat <- function(
    data_dir = "data", 
    output_file = file.path("manifest.json")) {
  
  all_entries <- 
    fs::dir_ls(data_dir, recurse = TRUE, all = TRUE, type = "file") |>
    stringr::str_subset("(^|/)[.][^/]+", negate = TRUE)
  
  entries <- list()
  
  for (entry in all_entries) {
    rel_path <- fs::path_rel(entry, start = ".")
    info <- fs::file_info(entry)
    is_dir <- fs::is_dir(entry)
    entry_data <- list(
      path = as.character(rel_path),
      size = if (is_dir) "-" else info$size,
      mtime = if (is_dir) "-" else format(info$modification_time, "%Y-%Om-%d %H:%M:%S")
    )
    entries[[length(entries) + 1]] <- entry_data
  }
  
  # Sort by path
  entries <- entries[order(sapply(entries, function(x) x$path))]
  
  jsonlite::write_json(entries, output_file, pretty = TRUE, auto_unbox = TRUE)
  message("✅ Wrote ", length(entries), " entries to ", output_file)
}

# Generate the flat index
generate_tree_flat()

# Knit the readme
rmarkdown::render("README.Rmd")
