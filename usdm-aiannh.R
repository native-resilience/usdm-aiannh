# update.packages(repos = "https://cran.rstudio.com/",
#                 ask = FALSE)
# 
# install.packages("pak",
#                  repos = "https://mac.r-project.org")
# 
# options("pkg.cran_mirror" = "https://mac.r-project.org")
# 
# # installed.packages() |>
# #   rownames() |>
# #   pak::pkg_install(upgrade = TRUE,
# #                  ask = FALSE)
# 
# pak::pak(
#   c(
#     "arrow?source",
#     "sf?source",
#     "curl",
#     "tidyverse",
#     "tigris",
#     "rmapshaper",
#     "furrr",
#     "future.mirai"
#   )
# )

library(magrittr)
library(tidyverse)
library(sf)
library(arrow)
library(furrr)
library(future.mirai)

## The archive of record is s3://native-resilience/usdm-aiannh/, served at
## https://data.native-resilience.com/usdm-aiannh/. PUBLISH=0 builds locally
## without uploading.
source("R/s3-archive.R")
s3_preflight()
s3_bucket_name <- Sys.getenv("S3_BUCKET", unset = "native-resilience")
s3_prefix      <- Sys.getenv("S3_PREFIX", unset = "usdm-aiannh")
publish        <- Sys.getenv("PUBLISH", unset = "1") != "0"
## Pull prior archive state so incremental guards see existing outputs. Only
## the weekly determinations need it — the guard below is !file.exists() over
## data/usdm-aiannh — and pulling all of data/ would drag down the retired
## boundary copy described under `aiannh`, then hand it straight back to S3.
s3_pull(s3_bucket_name, paste0(s3_prefix, "/data/usdm-aiannh"),
        "data/usdm-aiannh")

sf::sf_use_s2(TRUE)

## The AIANNH boundaries come from the census-aiannh archive, which owns the
## TIGER/Line downloads, the per-vintage schema normalization and the validity
## repair. This repo used to build them itself (2025 only) and publish a second
## copy under its own prefix; the weekly determinations below are the only
## reason this repo ever needed them. The unit is the GEOID component:
## reservation (R) and off-reservation trust land (T) stay separate rows.
##
## They are cached under data-raw/ rather than read from the CDN per task: the
## intersections run in parallel across ~1,400 weeks and every worker reads its
## week's vintage. The TIGER vintage is kept as census_year, so each worker
## sees exactly the schema the determinations are written against.
##
## Unlike usdm-counties, the boundaries stay in their published NAD83
## (EPSG:4269) and each week's USDM layer is transformed to them: the archive's
## Area was measured on the NAD83 geometry, and transforming the boundaries to
## EPSG:4326 first shifts some components' s2 area by up to 1e-4 of Area —
## enough that the class percentages no longer sum to 1.
##
## Vintages are discovered from the archive rather than hardcoded, so a new
## TIGER release flows through without an edit here.
CENSUS_AIANNH <-
  Sys.getenv("CENSUS_AIANNH_URL",
             unset = "https://data.sustainable-fsa.com/census-aiannh")

dir.create(
  file.path("data-raw","census"),
  recursive = TRUE,
  showWarnings = FALSE
)

dir.create(
  file.path("data","usdm-aiannh"),
  recursive = TRUE,
  showWarnings = FALSE
)

aiannh <-
  c(2000, 2007:(lubridate::year(lubridate::today()) + 1)) %>%
  magrittr::set_names(., .) %>%
  purrr::map_chr(
    \(x){
      file.path(CENSUS_AIANNH, "data", "parquet",
                paste0(x, "-aiannh.parquet"))
    }
  ) %>%
  .[purrr::map_lgl(., url_exists)] %>%
  purrr::imap_chr(\(x, year){
    outfile <-
      file.path("data-raw","census", paste0(year,"-aiannh.parquet"))

    if(!file.exists(outfile))
      x %>%
      sf::read_sf() %>%
      dplyr::select(GEOID, AIANNHCE, GNIS, Name, NameLSAD, LSAD, COMPTYP,
                    census_year = year, Area) %>%
      sf::write_sf(
        outfile,
        driver = "Parquet",
        layer_options = c("COMPRESSION=ZSTD",
                          "COMPRESSION_LEVEL=13"),
        delete_dsn = TRUE
      )

    return(outfile)
  }) %>%
  {
    tibble::tibble(
      Year = as.integer(names(.)) + 1,
      AIANNH = .)
  } %>%
  tidyr::complete(Year = 2000:(lubridate::year(lubridate::today()))) %>%
  tidyr::fill(AIANNH) %>%
  tidyr::fill(AIANNH, .direction = "up")

usdm_get_dates <-
  function(as_of = lubridate::today("America/Denver")){
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
        "https://data.sustainable-fsa.com/usdm",
        "data", "parquet",
        paste0("USDM_",Date,".parquet")),
    outfile = file.path("data", "usdm-aiannh",
                        paste0("USDM_",Date,".parquet"))
  ) %>%
  dplyr::left_join(aiannh) %>%
  dplyr::filter(!file.exists(outfile)) %>%
  ## Freshness gate: drop weeks whose upstream usdm parquet isn't published
  ## yet (fallback/premature runs no-op instead of failing in read_sf).
  (function(df){
    posted <- purrr::map_lgl(df$USDM, url_exists)
    purrr::walk(df$Date[!posted],
                function(d) gate_skip(paste0("Upstream usdm parquet for ", d,
                                             " not yet published; skipping.")))
    df[posted, ]
  }) %>%
  furrr::future_pwalk(
    .f = function(Date,
                  USDM,
                  AIANNH,
                  outfile,
                  ...){

      cat(USDM)

      if(!file.exists(outfile)){
        aiannh <-
          AIANNH %>%
          sf::read_sf() %>%
          sf::`st_agr<-`("constant")

        usdm <-
          USDM %>%
          sf::read_sf() %>%
          sf::st_transform(sf::st_crs(aiannh)) %>%
          sf::`st_agr<-`("constant")

        out <-
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
          ## Areas are measured on the s2 overlay output as is: it is valid by
          ## construction, and the st_cast/st_make_valid round trip
          ## usdm-counties applies here moves percentages by up to 3e-5.
          ## Zero-area pieces (shared edges or points) are not drought cover.
          dplyr::mutate(
            usdm_date = Date,
            usdm_class =
              tidyr::replace_na(usdm_class, "None") %>%
              factor(levels = c("None", paste0("D", 0:4)),
                     ordered = TRUE),
            usdm_percent = units::drop_units(sf::st_area(geometry) / Area)
          ) %>%
          dplyr::filter(usdm_percent > 0) %>%
          dplyr::select(GEOID, AIANNHCE, GNIS, Name, NameLSAD, LSAD, COMPTYP,
                        census_year, usdm_date, usdm_class, usdm_percent) %>%
          dplyr::arrange(GEOID, usdm_class) %>%
          sf::st_drop_geometry()

        ## QA, beyond usdm-counties: every component of the week's vintage is
        ## present, once per class, and its classes tile it. The tolerance is
        ## floating-point slack on the s2 areas, not room for a missing piece.
        sums <- tapply(out$usdm_percent, out$GEOID, sum)
        stopifnot(
          !anyDuplicated(out[c("GEOID", "usdm_class")]),
          setequal(out$GEOID, aiannh$GEOID),
          all(abs(sums - 1) < 1e-6)
        )

        out %>%
          arrow::write_parquet(sink = outfile,
                               version = "latest",
                               compression = "zstd",
                               compression_level = 13,
                               use_dictionary = TRUE)
      }
    }
  )

plan(sequential)

## Create a single parquet output, for simplicity
usdm_aiannh <-
  list.files("data/usdm-aiannh",
             recursive = TRUE,
             full.names = TRUE) %>%
  purrr::map_dfr(arrow::read_parquet) %>%
  dplyr::arrange(GEOID, usdm_date, usdm_class)

usdm_aiannh %>%
  arrow::write_parquet(sink = "usdm-aiannh.parquet",
                       version = "latest",
                       compression = "zstd",
                       compression_level = 13,
                       use_dictionary = TRUE)

## Browser-optimized JSON mirror of the weekly worst (max) USDM class per
## AIANNH component, for web maps: the full area-percent detail stays in the
## parquet; the browser needs only "how bad was this area this week." One
## fixed-width string per component — one character per USDM Tuesday, '.'
## where the component is absent from that week's vintage — is ~10x smaller
## raw than parallel arrays and decodes with a single charCodeAt. Worst class
## means max(usdm_class) over every row present, no percent threshold,
## matching usdm-counties: any nonzero-area sliver counts.
## The schema is a frozen contract — add fields; never rename or reorder
## existing ones without bumping "usdm-max-class-aiannh/1". It is
## usdm-max-class/1 (usdm-counties.json) with the county arrays (counties,
## county_names, state_names) replaced by geoids and names.
##
## usdm-max-class-aiannh/1 decode:
##   const d = await (await fetch('usdm-aiannh.json')).json();
##   // week j (0-based, j < d.weeks) is the Tuesday d.week0 + 7*j days:
##   const t = Date.parse(d.week0) + j * 7 * 86400000;
##   // worst USDM class for component i (d.geoids[i], 5-char GEOID) at week j:
##   const ch = d.series[i][j];
##   if (ch === '.') { /* component not in this week's archive vintage */ }
##   else label = d.classes[ch.charCodeAt(0) - 48];   // 'None','D0'..'D4'
##   // display: d.names[i] (NameLSAD as of the component's latest week)
##   // integrity: total non-'.' chars across d.series === d.n
web_classes <- c("None", paste0("D", 0:4))
web_week0 <- lubridate::as_date("2000-01-04")

stopifnot(identical(levels(usdm_aiannh$usdm_class), web_classes))
web_max <-
  usdm_aiannh %>%
  dplyr::transmute(
    geoid = GEOID,
    usdm_date,
    class = as.integer(usdm_class) - 1L
  ) %>%
  dplyr::group_by(geoid, usdm_date) %>%
  dplyr::summarise(class = max(class), .groups = "drop") %>%
  dplyr::arrange(geoid, usdm_date)

## The week axis is the unbroken Tuesday grid; a component-week the archive
## does not carry becomes '.', never an imputed value.
stopifnot(min(web_max$usdm_date) == web_week0,
          all(as.integer(web_max$usdm_date - web_week0) %% 7L == 0L))
web_weeks <- as.integer(max(web_max$usdm_date) - web_week0) %/% 7L + 1L

## Radix sort is the C locale, so the file is byte-identical whatever
## locale the runner happens to be in.
web_geoids <- sort(unique(web_max$geoid), method = "radix")

web_grid <- matrix(".", nrow = length(web_geoids), ncol = web_weeks)
web_grid[cbind(match(web_max$geoid, web_geoids),
               as.integer(web_max$usdm_date - web_week0) %/% 7L + 1L)] <-
  as.character(web_max$class)
web_series <-
  vapply(seq_along(web_geoids),
         function(i) paste(web_grid[i, ], collapse = ""),
         character(1))

## The digit-and-dot strings must reconstruct the max-class table exactly;
## a lossy encoding would be invisible in the browser.
stopifnot(identical(
  tibble::tibble(
    geoid = rep(web_geoids, each = web_weeks),
    usdm_date = rep(web_week0 + 7L * (seq_len(web_weeks) - 1L),
                    times = length(web_geoids)),
    class = unlist(strsplit(web_series, "", fixed = TRUE), use.names = FALSE)
  ) %>%
    dplyr::filter(class != ".") %>%
    dplyr::mutate(class = as.integer(class)),
  web_max
))

## Display names for the dictionary only: each GEOID's name as recorded at
## its most recent week.
web_names <-
  usdm_aiannh %>%
  dplyr::transmute(geoid = GEOID, usdm_date, NameLSAD) %>%
  dplyr::group_by(geoid) %>%
  dplyr::slice_max(usdm_date, n = 1, with_ties = FALSE) %>%
  dplyr::ungroup()

## census-aiannh's names arrive as clean UTF-8 (no double latin1 encoding to
## undo, unlike the county names), so this only guards that it stays so.
stopifnot(!any(grepl("\u00c3", web_names$NameLSAD)))

stopifnot(!anyDuplicated(web_names$geoid),
          nrow(web_names) == length(web_geoids),
          !anyNA(web_names$NameLSAD))
web_names <- web_names[match(web_geoids, web_names$geoid), ]

jsonlite::write_json(
  list(
    schema = jsonlite::unbox("usdm-max-class-aiannh/1"),
    dataset = jsonlite::unbox("usdm-aiannh"),
    license = jsonlite::unbox("CC0-1.0"),
    classes = web_classes,
    week0 = jsonlite::unbox(format(web_week0)),
    weeks = jsonlite::unbox(web_weeks),
    geoids = web_geoids,
    names = web_names$NameLSAD,
    n = jsonlite::unbox(nrow(web_max)),
    series = web_series
  ),
  "usdm-aiannh.json",
  auto_unbox = FALSE, digits = NA
)

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

## ---- Publish to S3 ---------------------------------------------------
## delete = TRUE is an exact mirror of data/ (pattern of usdm-counties), so
## rebuilt weeks overwrite in place and the retired data/census/ copy goes.
## The combined table, web JSON and manifests are rewritten each run and
## invalidated.
if (publish) {
  s3_push(s3_bucket_name, paste0(s3_prefix, "/data"), "data", delete = TRUE)
  s3_put(s3_bucket_name, paste0(s3_prefix, "/usdm-aiannh.parquet"),
         "usdm-aiannh.parquet",
         content_type = "application/vnd.apache.parquet",
         cache_control = "max-age=3600")
  s3_put(s3_bucket_name, paste0(s3_prefix, "/usdm-aiannh.json"),
         "usdm-aiannh.json",
         content_type = "application/json",
         cache_control = "max-age=3600")
  s3_put(s3_bucket_name, paste0(s3_prefix, "/manifest.json"), "manifest.json",
         content_type = "application/json",
         cache_control = "max-age=3600")
  s3_verify(s3_bucket_name, paste0(s3_prefix, "/data"), "data",
            allow_extra = character(0))
  s3_write_manifest(s3_bucket_name, s3_prefix)
  cf_invalidate(paste0("/", s3_prefix, c("/usdm-aiannh.parquet",
                                         "/usdm-aiannh.json",
                                         "/manifest.json",
                                         "/_manifest.txt")))
  cf_wait_manifest(
    paste0(Sys.getenv("CLOUDFRONT_BASE",
                      unset = "https://data.native-resilience.com"),
           "/", s3_prefix, "/manifest.json"),
    "manifest.json")
}

# ---- Render the README ----
# Regenerates README.md and the example map from the published archive; the
# workflow commits these (and only these, plus the web JSON) back to git.
# (rmarkdown reaches the conda env only transitively; guard it)
if (!requireNamespace("rmarkdown", quietly = TRUE))
  install.packages("rmarkdown", repos = "https://cloud.r-project.org")
rmarkdown::render("README.Rmd")
