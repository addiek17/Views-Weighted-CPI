### Install necessary packages ###
wants <-
  c(
    "tidyr",
    "lubridate",
    "tibble",
    "data.table",
    "knitr",
    "caret",
    "boot",
    "ggplot2",
    "gridExtra",
    "pander",
    "ggthemes",
    "scales",
    "foreign",
    "magrittr",
    "reshape2",
    "glmnet",
    'mgcv',
    'cvTools',
    'rpart',
    'class',
    'psych',
    'stringr',
    'sqldf',
    'rpart.plot',
    'randomForest',
    'corrplot',
    'bit64',
    "dplyr",
    "readxl",
    'ngram',
    'stringi',
    'stringdist',
    'fuzzyjoin',
    'mgsub',
    'tidytext',
    'plotly'
  )

has   <- wants %in% rownames(installed.packages())
if (any(!has))
  install.packages(wants[!has])
lapply(wants, library, character.only = T)
library(odbc)
library(rstudioapi)
library(DBI)
library(naniar)

### remove scientific notation
options(scipen = 999)

### load file with my functions ###
setwd(
  "//cs01corp/root/files/corp/MKT/950240/PUB-DB/Pricing Strategy/Addie/New CPI Metric Analysis"
)

source("functions.r")


### teradata pull ### review and adjust dates if necessary (originally pulling from beginning of calendar 2018)
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver = "Teradata",
  Authentication   = "LDAP",
  DBCName = "",
  UID    = '',
  PWD    = ''
)

currentCPI  = dbGetQuery(
  con,
  "SELECT x.*
,round(R_PRODUCT/BBY_UNIT_PRODUCT*100,2) as CPI
FROM
(
SELECT
	a.CLASS_ID,
	a.FISC_EOW_DT,
	cal.fisc_wk_of_mth_id,
	SUM(BBY_UNIT_PRODUCT) as BBY_UNIT_PRODUCT,
	SUM(R_PRODUCT) as R_PRODUCT
	FROM PRODBBYMERADHOCDB.DAILY_CPI_FNL_TABLEAU_V2 a
	left join (select distinct calndr_dt,fisc_wk_of_mth_id,fisc_eow_dt from prodbbyrptvws.bby_fiscal_calendar) cal on a.fisc_eow_dt=cal.calndr_dt
	GROUP BY a.CLASS_ID, a.FISC_EOW_DT,cal.fisc_wk_of_mth_id
	where BBY_UNIT_PRODUCT>0
	) x
"
)

setnames(currentCPI, names(currentCPI), tolower(names(currentCPI)))


###Load in NPD data
library(data.table)
files <-
  list.files(path = "//cs01corp/Root/Files/Corp/MKT/950240/PUB-DB/Pricing Strategy/Addie/Correct ", pattern = ".csv")
temp <-
  lapply(files, read.csv, row.names = NULL)
data <- rbindlist(temp)
data = data %>% rename(industry = Industry)#%>%rename(Industry.Segment=industry_seg)

setnames(
  data,
  c(
    'Industry.Segment',
    'Category.Group',
    'Category',
    'Sub.Category'
  ),
  c('industry_seg', 'cat_group', 'category', 'sub_cat')
)#,skip_absent = TRUE)



##Load in Reference File
setwd(
  "//cs01corp/Root/Files/Corp/MKT/950240/PUB-DB/Pricing Strategy/Addie"
)
reference <-
  read.csv(file = "Reference1.csv", header = TRUE)

reference$cat_group = as.character(reference$cat_group)

reference = reference %>% distinct(
  class_nm,
  class_id,
  industry,
  industry_seg,
  cat_group,
  category,
  sub_cat
)

reference = reference %>% mutate(
  cat_group = ifelse(cat_group == 'Color Television', 'Televisions', cat_group)
)



data1 = merge(
  data,
  reference,
  by.x = c("industry", "industry_seg", "cat_group", "category", "sub_cat"),
  by.y = c("industry", "industry_seg", "cat_group", "category", "sub_cat")
)
data1$class_id = as.character(data1$class_id)

setnames(data1, names(data1), tolower(names(data1)))



##Load in Views Weighted CPI Data
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver = "Teradata",
  Authentication   = "LDAP",
  DBCName = "",
  UID    = '',
  PWD    = ''
)

views_weighted = dbGetQuery(
  con,
  "select
class_id,
date_time,
fisc_eow_dt,
fisc_wk_of_mth_id,
bby_curr_sale_prc_amt*visitor_count as bby_views_prod,
comp_price*visitor_count as r_views_prod,
sum(visitor_count) visitor_count
from
(select
C.sku_id,
C.class_id,
a.date_time,
cal.fisc_wk_of_mth_id,
B.bby_curr_sale_prc_amt,
cal.fisc_eow_dt,
--B.rlv_cmptr_prc_amt,
--B.prm_cmptr_prc_amt ,
--B.sec_cmptr_prc_amt,
coalesce(B.rlv_cmptr_prc_amt,B.prm_cmptr_prc_amt,B.sec_cmptr_prc_amt) as comp_price,
sum(A. visitor_count) visitor_count
from prodbbymeradhocdb.bdp_vws_mkt_vehicle_final A
left join PRODBBYRPTVWS.ALL_CHANNEL_MERCH_VEND_CURRENT C
on A.sku_id=C.sku_id and c.chn_id=160
left join prodbbyrptvws.cpi_online_weekly_snapshot B
on A.sku_id=B.sku_id and a.date_time=b.snpsht_dt and b.chn_id=160
left join (select distinct calndr_dt,fisc_wk_of_mth_id,fisc_eow_dt from prodbbyrptvws.bby_fiscal_calendar) cal on a.date_time=cal.calndr_dt
where date_time>=1170101
group by 1,2,3,4,5,6,7) x
group by 1,2,3,4,5,6
"
)

setnames(views_weighted, names(views_weighted), tolower(names(views_weighted)))

views_weighted$fisc_eow_dt = as.Date(views_weighted$fisc_eow_dt, "%m/%d/%y")

views_weighted = views_weighted %>% rename(fisc_wk =
                                             fisc_wk_of_mth_id)

##Date Reformating

data1$date = str_sub(data1$week, start = 1, end =
                       11) # instead of time.period.s. use the column name of the NPD date
setwd(
  '\\\\cs01corp/root/Files/Corp/MKT/950240/PUB-DB/Pricing Strategy/Artem/Market Share data check/Apple Market Share'
)
week_hier = read.csv('week hierarchy.csv')
#setnames(week_hier, names(week_hier), tolower(names(week_hier)))
#setnames(week_hier,'begin.dt','date')
x = merge(data1, week_hier, by.x = 'date', by.y =
            'Begin.Dt') # the first datatest here is your dataset that you want to change the dates in, the second one keep as is.

npd_all = x %>% ungroup() %>%
  mutate(class_id = ifelse(
    class_id %in% c(276, 478, 479, 306, 500),
    '276, 478, 479, 306, 500',
    ifelse(class_id %in% c(124, 544), '124, 544 mobile', class_id)
  ))

npd_bby = npd_all %>% filter(outlet.family == 'Best Buy') %>%
  group_by(fisc_wk, class_id) %>% summarise(dollars = sum(dollars), units =
                                              sum(units))
npd_rr = npd_all %>% filter(outlet.family == 'Remaining Retail')


currentCPI = currentCPI %>% rename(fisc_wk = fisc_wk_of_mth_id) %>%
  drop_na(class_id)


currentCPI1 = currentCPI %>% ungroup() %>%
  mutate(class_id = ifelse(
    class_id %in% c(276, 478, 479, 306, 500),
    '276, 478, 479, 306, 500',
    ifelse(class_id %in% c(124, 544), '124, 544 mobile', class_id)
  ))

currentCPI2 = currentCPI1 %>% group_by(class_id, fisc_wk) %>%
  summarize(
    bby_unit_product = sum(bby_unit_product),
    r_product = sum(r_product)
  ) %>%
  mutate(cpi = round((r_product / bby_unit_product) *
                       100, digits = 2
  )) %>% drop_na(cpi)




### Creating a views weighted CPI

views_weighted$r_views_prod = as.numeric(views_weighted$r_views_prod)


views_weighted$bby_views_prod = as.numeric(views_weighted$bby_views_prod)


views_weighted1 = views_weighted %>% mutate(class_id =
                                              ifelse(
                                                class_id %in% c(276, 478, 479, 306, 500),
                                                '276, 478, 479, 306, 500',
                                                ifelse(class_id %in% c(124, 544), '124, 544 mobile', class_id)
                                              ))

views_weighted2 = views_weighted1 %>% group_by(fisc_wk, class_id) %>%
  summarize(
    r_views_prod = sum(r_views_prod, na.rm = T),
    bby_views_prod = sum(bby_views_prod, na.rm = T)
  ) %>%
  mutate(weighted_cpi = round((r_views_prod / bby_views_prod) *
                                100, digits = 2
  )) %>% drop_na(weighted_cpi)

comb = merge(
  views_weighted2,
  currentCPI2,
  by.y = c("class_id", "fisc_wk"),
  by.x = c("class_id", "fisc_wk")
)



npd_bby = npd_bby %>% group_by(class_id, fisc_wk)

unique(npd_bby$class_id)


fnl = merge(
  comb,
  npd_bby,
  by.y = c("class_id", "fisc_wk"),
  by.x = c("class_id", "fisc_wk")
)

fnl = fnl %>% group_by(fisc_wk, class_id)




weighted_cpi = fnl$weighted_cpi
cpi = fnl$cpi
cor(weighted_cpi, cpi)  