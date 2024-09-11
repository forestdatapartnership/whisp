Source list behind lookup_gee_datasets.csv

**Tree cover datasets:**

| Dataset name | Description of zonal statistics applied | Source and GEE asset|
| -- | -- | -- |
| EUFO_2020 | Binary values, where 1 is forest. | *Bourgoin, Clement; Ameztoy, Iban; Verhegghen, Astrid; Carboni, Silvia; Colditz, Rene R.; Achard, Frederic (2023):* **Global map of forest cover 2020 - version 1**. European Commission, Joint Research Centre (JRC) [Dataset] PID: http://data.europa.eu/89h/10d1b337-b7d1-4938-a048-686c8185b290. <br><br> **GEE:** ee.ImageCollection(“JRC/GFC2020/V1”) |
| GLAD_Primary | Binary input layer representing primary forest in 2001. Loss pixels 2001-2020 removed with ancillary GFC dataset. | *Turubanova S., Potapov P., Tyukavina, A., and Hansen M. (2018):* **Ongoing primary forest loss in Brazil, Democratic Republic of the Congo, and Indonesia.** Environmental Research Letters. DOI: [10.1088/1748-9326/aacd1c](https://doi.org/10.1088/1748-9326/aacd1c) <br><br> **GEE:** ee.ImageCollection (‘UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1’)  <br> **GEE ancillary:** ee.Image(“UMD/hansen/global_forest_change_2022_v1_10”)|
| TMF_undist | Select tropical moist forest classes (10, 11 & 12) in 2020 representing undisturbed cover. | *C. Vancutsem, F. Achard, J.-F. Pekel, G. Vieilledent, S. Carboni, D. Simonetti, J. Gallego, L.E.O.C. Aragão, R. Nasi (2021):* **Long-term (1990-2019) monitoring of forest cover changes in the humid tropics.** Science Advances. DOI: [10.1126/sciadv.abe1603](https://doi.org/10.1126/sciadv.abe1603) <br><br> **GEE:** ee.ImageCollection(‘projects/JRC/TMF/TransitionMap_Subtypes’) |
| -- | -- | -- | check the above and below again |
| JAXA_FNF_2020 | Select dense and non-dense forest classes (i.e., classes 1 and 2), and data from the year 2020. | *Masanobu Shimada, Takuya Itoh, Takeshi Motooka, Manabu Watanabe, Shiraishi Tomohiro, Rajesh Thapa, and Richard Lucas (2014):* **New Global Forest/Non-forest Maps from ALOS PALSAR Data (2007-2010)**. Remote Sensing of Environment, 155, pp. 13-31, December 2014. DOI: [10.1016/j.rse.2014.04.014](https://doi.org/10.1016/j.rse.2014.04.014). <br><br> **GEE:** ee.ImageCollection(‘JAXA/ALOS/PALSAR/YEARLY/FNF4’)|
| GFC_TC_2020 | Select areas of tree cover over 10 percent and remove loss pixels between. | *Hansen, M. C., P. V. Potapov, R. Moore, M. Hancher, S. A. Turubanova, A. Tyukavina, D. Thau, S. V. Stehman, S. J. Goetz, T. R. Loveland, A. Kommareddy, A. Egorov, L. Chini, C. O. Justice, and J. R. G. Townshend (2013):* **High-Resolution Global Maps of 21st-Century Forest Cover Change.** Science 342 (15 November): 850-53. DOI: [10.1126/science.1244693](https://doi.org/10.1126/science.1244693). Data available on-line from: https://glad.earthengine.app/view/global-forest-change. <br><br> **GEE:** ee.Image(“UMD/hansen/global_forest_change_2022_v1_10”) |
| GLAD_LULC_2020 | Select classes from the 2020 land cover map representing stable tree cover over 5m in height (i.e. classes 27 to 48, and 127 to 148). | *Potapov P., Hansen M.C., Pickens A., Hernandez-Serna A., Tyukavina A., Turubanova S., Zalles V., Li X., Khan A., Stolle F., Harris N., Song X.-P., Baggett A., Kommareddy I., Kommareddy A. (2022):* **The global 2000-2020 land cover and land use change dataset derived from the Landsat archive: first results.** Frontiers in Remote Sensing. DOI: [10.3389/frsen.2022.856903](https://doi.org/10.3389/frsen.2022.856903) <br><br>  **GEE:** ee.Image(‘projects/glad/GLCLU2020/v2/LCLUC_2020’)|
| ESA_TC_2020 | Select trees and mangrove classes (i.e., 10 and 95) for 2020. | *Zanaga, D., Van De Kerchove, R., De Keersmaecker, W., Souverijns, N., Brockmann, C., Quast, R., Wevers, J., Grosu, A., Paccini, A., Vergnaud, S., Cartus, O., Santoro, M., Fritz, S., Georgieva, I., Lesiv, M., Carter, S., Herold, M., Li, Linlin, Tsendbazar, N.E., Ramoino, F., Arino, O., 2021:* **ESA WorldCover 10 m 2020 v100.** DOI: [10.5281/zenodo.5571936](https://doi.org/10.5281/zenodo.5571936) <br><br> **GEE:** ee.Image(“ESA/WorldCover/v100/2020”)|
| ESRI_TC_2020 | -- | *K. Karra, C. Kontgis, Z. Statman-Weil, J. C. Mazzariello, M. Mathis and S. P. Brumby (2021):* **Global land use / land cover with Sentinel 2 and deep learning.** IEEE International Geoscience and Remote Sensing Symposium IGARSS, Brussels, Belgium, 2021, pp. 4704-4707, DOI: [10.1109/IGARSS47720.2021.9553499](https://doi.org/10.1109/IGARSS47720.2021.9553499) <br><br>**GEE:** ee.ImageCollection("projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS") |

<br><br><br>

**Commodity datasets:**
| Dataset name | Description of zonal statistics applied | Citation |
| -- | -- | -- |
| TMF_plant | Select classes representing any type of plantation (i.e., classes 81-86). | *C. Vancutsem, F. Achard, J.-F. Pekel, G. Vieilledent, S. Carboni, D. Simonetti, J. Gallego, L.E.O.C. Aragão, R. Nasi (2021):* **Long-term (1990-2019) monitoring of forest cover changes in the humid tropics.** Science Advances. DOI: [10.1126/sciadv.abe1603](https://doi.org/10.1126/sciadv.abe1603) <br><br> **GEE:** ee.ImageCollection(‘projects/JRC/TMF/v1_2021/TransitionMap_Subtypes’) |
| Oil_palm_Descals | Mosaic image collection into a single image. Selected classes from the “classification band” representing Industrial and Smallholder closed-canopy oil palm plantations( i.e. classes 0 and 1). | Descals, A., Wich, S., Meijaard, E., Gaveau, D. L. A., Peedell, S., and Szantoi, Z.: High-resolution global map of smallholder and industrial closed-canopy oil palm plantations, Earth Syst. Sci. Data, 13, 1211–1231, https://doi.org/10.5194/essd-13-1211-2021, 2021. <br><br> **GEE:** ee.ImageCollection(‘BIOPAMA/GlobalOilPalm/v1’) |
| Oil_palm_FDaP | Binary layer. For select countries only. | FDaP. 2024. Lookup GEE datasets. In: Forest Data Partnership – GitHub. [Cited 17 January 2024]. https://github.com/forestdatapartnership/whisp/blob/main/parameters/lookup_gee_datasets.csv <br><br> **GEE:** ee.ImageCollection(“projects/forestdatapartnership/assets/palm/palm_2020_model_20231026”)|
| Cocoa_ETH | Binary product where 1 represents cocoa. Product derived from a cocoa probability map where the recommended threshold of >65%, had already been applied. | Kalischek, N., Lang, N., Renier, C., Daudt, R. C., Addoah, T., Thompson, W., Blaser-Hart, W. J., Garrett, R. D., Schindler, K., & Wegner, J. D. (2022). Satellite-based high-resolution maps of cocoa planted area for Côte d'Ivoire and Ghana. <br><br> **GEE:** ee.Image(‘projects/ee-nk-cocoa/assets/cocoa_map_threshold_065’) |
| Cocoa_bnetd | -- | -- |

<br><br><br>

**Ancillary datasets:**
| Dataset name | GEE Asset ID | Description of zonal statistics applied | Citation |


<br><br><br>

**Disturbance before & after 2020 datasets:**
| Dataset name | GEE Asset ID | Description of zonal statistics applied | Citation |
| -- | -- | -- | -- |
| TMF_deg_before_2020 <br> TMF_def_before_2020 <br> TMF_deg_after_2020 <br> TMF_def_after_2020 <br>| ee.ImageCollection(‘projects/JRC/TMF/TransitionMap_Subtypes’) | Select classes representing tree cover classified as degraded, regrowth or with some ongoing deforestation in 2020 (i.e., classes 21-26; 61-62; 31-33; 63-64; 51-54; 67 and 92-94) | *C. Vancutsem, F. Achard, J.-F. Pekel, G. Vieilledent, S. Carboni, D. Simonetti, J. Gallego, L.E.O.C. Aragão, R. Nasi (2021):* **Long-term (1990-2019) monitoring of forest cover changes in the humid tropics.** Science Advances. DOI: [10.1126/sciadv.abe1603](https://doi.org/10.1126/sciadv.abe1603) <br><br> **GEE:** ee.ImageCollection(‘projects/JRC/TMF/TransitionMap_Subtypes’) |
| GFC_loss_before_2020 <br> GFC_loss_after_2020| ee.Image(“UMD/hansen/global_forest_change_2022_v1_10”) | Select loss of the years in question. | Hansen, M. C., P. V. Potapov, R. Moore, M. Hancher, S. A. Turubanova, A. Tyukavina, D. Thau, S. V. Stehman, S. J. Goetz, T. R. Loveland, A. Kommareddy, A. Egorov, L. Chini, C. O. Justice, and J. R. G. Townshend. 2013. High-Resolution Global Maps of 21st-Century Forest Cover Change. Science 342 (15 November): 850-53. Data available on-line from: https://glad.earthengine.app/view/global-forest-change. |
| RADD_before_2020 <br> RADD_after_2020| ee.ImageCollection(‘projects/radar-wur/raddalert/v1’) Ancillary data: ee.ImageCollection(‘UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1’) | Select confirmed (i.e., class 3) alerts since 2020. Alerts filtered to within forest using the ancillary dataset (GLAD Primary). | Reiche J, Mullissa A, Slagter B, Gou Y, Tsendbazar N, Odongo-Braun C, Vollrath A, Weisse M, Stolle F, Pickens A, Donchyts G, Clinton N, Gorelick N & Herold M, (2021), Forest disturbance alerts for the Congo Basin using Sentinel-1, Environmental Research Letters, https://doi.org/10.1088/1748-9326/abd0a8. |
| MODIS_fire_before_2020 <br> MODIS_fire_after_2020 | -- | -- | Giglio, L., Justice, C. O., Boschetti, L., & Roy, D. P. (2020). MCD14DL MODIS/Terra and Aqua Thermal Anomalies/Fire 1km (V6) [Data set]. NASA LP DAAC. |
| ESA_fire_before_2020 | -- | -- | -- |


dataset_id,dataset_order,dataset_name,presence_only_flag,exclude,theme,use_for_risk
1,10,EUFO_2020,0,0,treecover,1
2,20,GLAD_Primary,0,0,treecover,1
3,30,TMF_undist,0,0,treecover,1
4,40,JAXA_FNF_2020,0,0,treecover,1
5,50,GFC_TC_2020,0,0,treecover,1
6,60,GLAD_LULC_2020,0,1,treecover,
7,70,ESA_TC_2020,0,0,treecover,1
8,80,ESRI_TC_2020,0,1,treecover,
9,90,TMF_disturbed,0,0,disturbance_before,
10,100,TMF_plant,0,0,commodities,1
11,110,Oil_palm_Descals,0,0,commodities,1
12,120,Oil_palm_FDaP,0,0,commodities,1
13,130,Cocoa_ETH,0,0,commodities,1
14,140,Cocoa_bnetd,0,0,commodities,1
15,150,WDPA,1,1,ancilliary,
16,160,OECM,1,1,ancilliary,
17,170,KBA,1,1,ancilliary,
18,180,TMF_def_2000,0,0,disturbance_before,
19,190,TMF_def_2001,0,0,disturbance_before,
20,200,TMF_def_2002,0,0,disturbance_before,
21,210,TMF_def_2003,0,0,disturbance_before,
22,220,TMF_def_2004,0,0,disturbance_before,
23,230,TMF_def_2005,0,0,disturbance_before,
24,240,TMF_def_2006,0,0,disturbance_before,
25,250,TMF_def_2007,0,0,disturbance_before,
26,260,TMF_def_2008,0,0,disturbance_before,
27,270,TMF_def_2009,0,0,disturbance_before,
28,280,TMF_def_2010,0,0,disturbance_before,
29,290,TMF_def_2011,0,0,disturbance_before,
30,300,TMF_def_2012,0,0,disturbance_before,
31,310,TMF_def_2013,0,0,disturbance_before,
32,320,TMF_def_2014,0,0,disturbance_before,
33,330,TMF_def_2015,0,0,disturbance_before,
34,340,TMF_def_2016,0,0,disturbance_before,
35,350,TMF_def_2017,0,0,disturbance_before,
36,360,TMF_def_2018,0,0,disturbance_before,
37,370,TMF_def_2019,0,0,disturbance_before,
38,380,TMF_def_2020,0,0,disturbance_before,
39,390,TMF_def_2021,0,0,disturbance_after,
40,400,TMF_def_2022,0,0,disturbance_after,
41,410,TMF_deg_2000,0,0,disturbance_before,
42,420,TMF_deg_2001,0,0,disturbance_before,
43,430,TMF_deg_2002,0,0,disturbance_before,
44,440,TMF_deg_2003,0,0,disturbance_before,
45,450,TMF_deg_2004,0,0,disturbance_before,
46,460,TMF_deg_2005,0,0,disturbance_before,
47,470,TMF_deg_2006,0,0,disturbance_before,
48,480,TMF_deg_2007,0,0,disturbance_before,
49,490,TMF_deg_2008,0,0,disturbance_before,
50,500,TMF_deg_2009,0,0,disturbance_before,
51,510,TMF_deg_2010,0,0,disturbance_before,
52,520,TMF_deg_2011,0,0,disturbance_before,
53,530,TMF_deg_2012,0,0,disturbance_before,
54,540,TMF_deg_2013,0,0,disturbance_before,
55,550,TMF_deg_2014,0,0,disturbance_before,
56,560,TMF_deg_2015,0,0,disturbance_before,
57,570,TMF_deg_2016,0,0,disturbance_before,
58,580,TMF_deg_2017,0,0,disturbance_before,
59,590,TMF_deg_2018,0,0,disturbance_before,
60,600,TMF_deg_2019,0,0,disturbance_before,
61,610,TMF_deg_2020,0,0,disturbance_before,
62,620,TMF_deg_2021,0,0,disturbance_after,
63,630,TMF_deg_2022,0,0,disturbance_after,
64,640,GFC_loss_year_2001,0,0,disturbance_before,
65,650,GFC_loss_year_2002,0,0,disturbance_before,
66,660,GFC_loss_year_2003,0,0,disturbance_before,
67,670,GFC_loss_year_2004,0,0,disturbance_before,
68,680,GFC_loss_year_2005,0,0,disturbance_before,
69,690,GFC_loss_year_2006,0,0,disturbance_before,
70,700,GFC_loss_year_2007,0,0,disturbance_before,
71,710,GFC_loss_year_2008,0,0,disturbance_before,
72,720,GFC_loss_year_2009,0,0,disturbance_before,
73,730,GFC_loss_year_2010,0,0,disturbance_before,
74,740,GFC_loss_year_2011,0,0,disturbance_before,
75,750,GFC_loss_year_2012,0,0,disturbance_before,
76,760,GFC_loss_year_2013,0,0,disturbance_before,
77,770,GFC_loss_year_2014,0,0,disturbance_before,
78,780,GFC_loss_year_2015,0,0,disturbance_before,
79,790,GFC_loss_year_2016,0,0,disturbance_before,
80,800,GFC_loss_year_2017,0,0,disturbance_before,
81,810,GFC_loss_year_2018,0,0,disturbance_before,
82,820,GFC_loss_year_2019,0,0,disturbance_before,
83,830,GFC_loss_year_2020,0,0,disturbance_before,
84,840,GFC_loss_year_2021,0,0,disturbance_after,
85,850,GFC_loss_year_2022,0,0,disturbance_after,
86,860,GFC_loss_year_2023,0,0,disturbance_after,
87,870,RADD_year_2019,0,0,disturbance_before,
88,880,RADD_year_2020,0,0,disturbance_before,
89,890,RADD_year_2021,0,0,disturbance_after,
90,900,RADD_year_2022,0,0,disturbance_after,
91,910,RADD_year_2023,0,0,disturbance_after,
92,920,RADD_year_2024,0,0,disturbance_after,
93,930,ESA_fire_2001,0,0,disturbance_before,
94,940,ESA_fire_2002,0,0,disturbance_before,
95,950,ESA_fire_2003,0,0,disturbance_before,
96,960,ESA_fire_2004,0,0,disturbance_before,
97,970,ESA_fire_2005,0,0,disturbance_before,
98,980,ESA_fire_2006,0,0,disturbance_before,
99,990,ESA_fire_2007,0,0,disturbance_before,
100,1000,ESA_fire_2008,0,0,disturbance_before,
101,1010,ESA_fire_2009,0,0,disturbance_before,
102,1020,ESA_fire_2010,0,0,disturbance_before,
103,1030,ESA_fire_2011,0,0,disturbance_before,
104,1040,ESA_fire_2012,0,0,disturbance_before,
105,1050,ESA_fire_2013,0,0,disturbance_before,
106,1060,ESA_fire_2014,0,0,disturbance_before,
107,1070,ESA_fire_2015,0,0,disturbance_before,
108,1080,ESA_fire_2016,0,0,disturbance_before,
109,1090,ESA_fire_2017,0,0,disturbance_before,
110,1100,ESA_fire_2018,0,0,disturbance_before,
111,1110,ESA_fire_2019,0,0,disturbance_before,
112,1120,ESA_fire_2020,0,0,disturbance_before,
113,1130,MODIS_fire_2001,0,0,disturbance_before,
114,1140,MODIS_fire_2002,0,0,disturbance_before,
115,1150,MODIS_fire_2003,0,0,disturbance_before,
116,1160,MODIS_fire_2004,0,0,disturbance_before,
117,1170,MODIS_fire_2005,0,0,disturbance_before,
118,1180,MODIS_fire_2006,0,0,disturbance_before,
119,1190,MODIS_fire_2007,0,0,disturbance_before,
120,1200,MODIS_fire_2008,0,0,disturbance_before,
121,1210,MODIS_fire_2009,0,0,disturbance_before,
122,1220,MODIS_fire_2010,0,0,disturbance_before,
123,1230,MODIS_fire_2011,0,0,disturbance_before,
124,1240,MODIS_fire_2012,0,0,disturbance_before,
125,1250,MODIS_fire_2013,0,0,disturbance_before,
126,1260,MODIS_fire_2014,0,0,disturbance_before,
127,1270,MODIS_fire_2015,0,0,disturbance_before,
128,1280,MODIS_fire_2016,0,0,disturbance_before,
129,1290,MODIS_fire_2017,0,0,disturbance_before,
130,1300,MODIS_fire_2018,0,0,disturbance_before,
131,1310,MODIS_fire_2019,0,0,disturbance_before,
132,1320,MODIS_fire_2020,0,0,disturbance_before,
133,1330,MODIS_fire_2021,0,0,disturbance_after,
134,1340,MODIS_fire_2022,0,0,disturbance_after,
135,1350,MODIS_fire_2023,0,0,disturbance_after,
136,1360,MODIS_fire_2024,0,0,disturbance_after,
137,1370,TMF_deg_before_2020,0,0,disturbance_before,1
138,1380,TMF_def_before_2020,0,0,disturbance_before,1
139,1390,GFC_loss_before_2020,0,0,disturbance_before,1
140,1400,ESA_fire_before_2020,0,0,disturbance_before,1
141,1410,MODIS_fire_before_2020,0,0,disturbance_before,1
142,1420,RADD_before_2020,0,0,disturbance_before,1
143,1430,TMF_deg_after_2020,0,0,disturbance_after,1
144,1440,TMF_def_after_2020,0,0,disturbance_after,1
145,1450,GFC_loss_after_2020,0,0,disturbance_after,1
146,1460,MODIS_fire_after_2020,0,0,disturbance_after,1
147,1470,RADD_after_2020,0,0,disturbance_after,1
