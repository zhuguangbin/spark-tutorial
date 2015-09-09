# This is a Demo for using SparkR

# setup SparkR env
Sys.setenv(SPARK_HOME = '/opt/spark-1.4.1-mv')
Sys.setenv(SPARK_YARN_QUEUE = "bi")
library("SparkR")

# init SparkR
sc = sparkR.init()
hiveContext = sparkRHive.init(sc)

# count cookies by province
cookiesByProvinceid_df = sql(
  hiveContext, "select geo_info.province as provinceid, count(cookie) as cookies from mediav_base.d_clickvalue where date='2015-07-07' and geo_info.country=1 group by geo_info.province"
)

cache(cookiesByProvinceid_df)
head(cookiesByProvinceid_df)

registerTempTable(cookiesByProvinceid_df,"cookiesByProvinceid")

# join mediav_base_location for provincename and geoid
cookiesByProvicename_df = sql(
  hiveContext,"select c.provinceid, c.cookies, l.en, l.geoid from cookiesByProvinceid c left join mysql.mediav_base_location l on c.provinceid=l.ID"
)

cache(cookiesByProvicename_df)
cookiesByProvicename = collect(cookiesByProvicename_df)

# geoid -> ADCODE99
ADCODE99 = substr(cookiesByProvicename$geoid,5,10)

# cbind, got a new data frame demonstrating num of cookies by province with province ADCODE99
cookiesByProvicename_withADCODE99 = cbind(cookiesByProvicename,ADCODE99)

cookiesByProvicename_withADCODE99


# using ggplot2 for plotting on china map

library(maptools)
library(ggplot2)
library(plyr)

gpclibPermit()

# load china map data
china_map = readShapePoly("mapdata/bou2_4p.shp")
# just plot it
plot(china_map)

x = china_map@data          #get location data
xs = data.frame(x,id = seq(0:924) - 1)          # 925 locations

china_map1 = fortify(china_map)           # fortify to data frame

china_map_data = join(china_map1, xs, type = "full")       #join two data frame

china_map_data$NAME = iconv(china_map_data$NAME, from = "GBK") # convert NAME to UTF8

head(china_map_data)

head(cookiesByProvicename_withADCODE99)

# join china_map_data with our cookies distribution data
cookiesByProvicename_map_data = join(china_map_data,cookiesByProvicename_withADCODE99, type =
                                       "right")

head(cookiesByProvicename_map_data)

# clean up NA
cookiesByProvicename_map_data[is.na(cookiesByProvicename_map_data)] <-
  0

# plot it, this is the final map of cookies distribution of china by province
viz = ggplot(cookiesByProvicename_map_data, aes(
  x = long, y = lat, group = group,fill = cookies
)) +
  geom_polygon(colour = "grey40") +
  scale_fill_gradient(low = "white",high = "steelblue") +  #指定渐变填充色，可使用RGB
  coord_map("polyconic") +       #指定投影方式为polyconic，获得常见视角中国地图
  theme(
    #清除不需要的元素
    panel.grid = element_blank(),
    panel.background = element_blank(),
    axis.text = element_blank(),
    axis.ticks = element_blank(),
    axis.title = element_blank(),
    legend.position = c(0.2,0.3)
  )

viz

# use plotly share it on web
library("plotly")
py <- plotly()
out = py$ggplotly(viz)
plotly_url = out$response$url
