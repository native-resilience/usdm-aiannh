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
    "arrow?source",
    "sf?source",
    "curl",
    "tidyverse",
    "tigris",
    "rmapshaper"
  )
)

library(magrittr)
library(tidyverse)
library(sf)
library(arrow)

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
    dplyr::mutate(`Total Area (m^2)` = sf::st_area(geometry)) %>%
    dplyr::select(GNIS, Name, NameLSAD, LSAD, `Total Area (m^2)`) %>%
    sf::write_sf(
      file.path("census-aiannh-2024.parquet"),
      driver = "Parquet",
      layer_options = c("COMPRESSION=BROTLI",
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
  function(as_of = lubridate::today(tzone = "America/Denver")){
    as_of %<>%
      lubridate::as_date()
    
    usdm_dates <-
      seq(lubridate::as_date("20000104"),
          lubridate::today(tzone = "America/Denver"), "1 week")
    
    usdm_dates <- usdm_dates[(as_of - usdm_dates) >= 2]
    
    return(usdm_dates)
  }

# library(furrr)
# future::plan(future.callr::callr)

out <-
  usdm_get_dates() %>%
  tibble::tibble(Date = .) %>%
  dplyr::mutate(
    USDM = 
      file.path("https://sustainable-fsa.github.io/usdm", 
                "usdm", "data", "parquet", 
                paste0("USDM_",Date,".parquet")),
    outfile = file.path("data", "usdm-aiannh", 
                        paste0("USDM_",Date,".parquet"))
  ) %>%
  dplyr::mutate(
    # `USDM AIANNH` = furrr::future_pmap_chr(
    `USDM AIANNH` = purrr::pmap_chr(
      .l = .,
      .f = function(USDM,
                    outfile, 
                    ...){
        
        if(!file.exists(outfile))
          
          sf::st_intersection(
            aiannh %>%
              sf::`st_agr<-`("constant"),
            USDM %>%
              sf::read_sf() %>%
              sf::`st_agr<-`("constant")
          ) %>%
          dplyr::rename(`Total Area (m^2)` = `Total.Area..m.2.`) %>%
          sf::st_cast("MULTIPOLYGON") %>%
          sf::st_make_valid() %>%
          dplyr::arrange(GNIS, Name, NameLSAD, LSAD, date, usdm_class) %>%
          dplyr::mutate(
            `USDM Class Area (m^2)` = units::drop_units(sf::st_area(geometry)),
            `USDM Class Area (%)` = 100 * `USDM Class Area (m^2)` / `Total Area (m^2)`
          ) %>%
          sf::st_drop_geometry() %>%
          dplyr::select(GNIS, Name, NameLSAD, LSAD, 
                        Date = date, `USDM Class` = usdm_class, 
                        `Total Area (m^2)`, `USDM Class Area (m^2)`,
                        `USDM Class Area (%)`) %>%
          dplyr::arrange(GNIS, Name, NameLSAD, `USDM Class`) %>%
          arrow::write_parquet(sink = outfile,
                               version = "latest",
                               compression = "zstd",
                               use_dictionary = TRUE)
        
        return(outfile)
      }
    )
  )

# future::plan(sequential)

arrow::open_dataset("data/usdm-aiannh/") %>%
  dplyr::collect() %>%
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
