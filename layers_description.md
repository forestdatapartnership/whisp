**Source list of the [layers used for risk evaluation](src/openforis_whisp/parameters/lookup_gee_datasets.csv)**

To view the layers in action, go to [https://whisp.earthmap.org/](https://whisp.earthmap.org/).


<table>
  <thead>
    <tr>
      <th style="width: 150px;">Dataset name</th>
      <th style="width: 250px;">Dataset description</th>
      <th style="width: 500px;">Source and GEE asset that the dataset is based on</th>
    </tr>
  </thead>
  <tbody>
    <tr><td colspan="3"><b>Tree cover datasets:</b></td></tr>
    <tr><td rowspan="3">EUFO_2020</td><td rowspan="3">Binary values, where 1 is forest.</td><td rowspan="2">Bourgoin, Clement; Verhegghen, Astrid; Degreve, Lucas; Ameztoy, Iban; Carboni, Silvia; Colditz, Rene; Achard, Frederic (2024) <i> Global map of forest cover 2020 </i> version 2. European Commission, Joint Research Centre (JRC) PID: http://data.europa.eu/89h/e554d6fb-6340-45d5-9309-332337e5bc26 </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“JRC/GFC2020/V2”)</td></tr>
    <tr><td rowspan="3">GLAD_Primary</td><td rowspan="3">Binary input layer representing primary forest in 2001. Loss pixels 2001-2020 removed with ancillary dataset.</td><td rowspan="2">Turubanova, S., Potapov, P. V., Tyukavina, A., & Hansen, M. C. (2018). <i>Ongoing primary forest loss in Brazil, Democratic Republic of the Congo, and Indonesia.</i> Environmental Research Letters, 13(7), 074028. https://doi.org/10.1088/1748-9326/aacd1c</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection (‘UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1’)<br>Ancillary: ee.Image("UMD/hansen/global_forest_change_2023_v1_11")</td></tr>
    <tr><td rowspan="3">TMF_undist</td><td rowspan="3">Mosaic for Dec 2020, representing undisturbed cover (class 1) .</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10). https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘projects/JRC/TMF/v1_2023/AnnualChanges’)</td></tr>
    <tr><td rowspan="3">GFC_TC_2020</td><td rowspan="3">Areas of tree cover over 10 percent in 2020 (loss pixels removed).</td><td rowspan="2">Hansen, M. C., Potapov, P. V., Moore, R., Hancher, M., Turubanova, S. A., Tyukavina, A., Thau, D., Stehman, S. V., Goetz, S. J., Loveland, T. R., Kommareddy, A., Egorov, A., Chini, L., Justice, C. O., & Townshend, J. R. G. (2013). <i>High-Resolution Global Maps of 21st-Century Forest Cover Change.</i> Science, 342(6160), 850–853. https://doi.org/10.1126/science.1244693. Data available online from: https://glad.earthengine.app/view/global-forest-change.</td></tr>
    <tr></tr>
    <tr><td>ee.Image("UMD/hansen/global_forest_change_2023_v1_11")
</td></tr>
    <tr><td rowspan="3">JAXA_FNF_2020</td><td rowspan="3">Dense and non-dense forest classes (i.e., 1 & 2) for 2020.</td><td rowspan="2">Shimada, M., Itoh, T., Motooka, T., Watanabe, M., Shiraishi, T., Thapa, R., & Lucas, R. (2014). <i>New global forest/non-forest maps from ALOS PALSAR data (2007–2010).</i> Remote Sensing of Environment, 155, 13–31. https://doi.org/10.1016/j.rse.2014.04.014</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘JAXA/ALOS/PALSAR/YEARLY/FNF4’)</td></tr>
    <tr><td rowspan="3">ESA_TC_2020</td><td rowspan="3">Tree and mangrove classes (i.e., 10 and 95) for 2020.</td><td rowspan="2">Zanaga, D., Van De Kerchove, R., De Keersmaecker, W., Souverijns, N., Brockmann, C., Quast, R., Wevers, J., Grosu, A., Paccini, A., Vergnaud, S., Cartus, O., Santoro, M., Fritz, S., Georgieva, I., Lesiv, M., Carter, S., Herold, M., Li, L., Tsendbazar, N.-E., Ramoino, F., Arino, O. (2021). <i>ESA WorldCover 10 m 2020 v100</i> (v100) [Dataset]. Zenodo. https://doi.org/10.5281/ZENODO.5571936</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("ESA/WorldCover/v100")</td></tr>
<tr><td rowspan="3">Forest_FDaP</td><td rowspan="3">Forest persistence for 2020 based on combining multiple forest/ tree cover datasets. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy.</td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/community_forests/ForestPersistence_2020")</td></tr>
    <tr><td colspan="3"><b>Commodity datasets:</b></td></tr>
    <tr><td rowspan="3">TMF_plant</td><td rowspan="3">Classes representing any type of plantation from transition map (classes 81-85). Deforestation data after 2020 removed so remaining areas represent plantations at end of 2020.</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10). https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection('projects/JRC/TMF/v1_2023/TransitionMap_Subtypes')<br>ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear')</td></tr>
    <tr><td rowspan="3">Oil_palm_Descals</td><td rowspan="3">Classes from the “classification band” representing oil palm plantations (i.e., 0 & 1).</td><td rowspan="2">Descals, A., Wich, S., Meijaard, E., Gaveau, D. L. A., Peedell, S., & Szantoi, Z. (2021). <i>High-resolution global map of smallholder and industrial closed-canopy oil palm plantations.</i> Earth System Science Data, 13(3), 1211–1231. https://doi.org/10.5194/essd-13-1211-2021</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(‘BIOPAMA/GlobalOilPalm/v1’)</td></tr>
    <tr><td rowspan="3">Oil_palm_FDaP</td><td rowspan="3">Palm probability model. Filtered collection to 2020 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy.</td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/palm/model_2024a")</td></tr>
        <tr><td rowspan="3">Cocoa_FDaP</td><td rowspan="3">Cocoa probability model. Filtered collection to 2020 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy.</td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/cocoa/model_2024a")</td></tr>
    <tr><td rowspan="3">Cocoa_ETH</td><td rowspan="3">Binary product where 1 represents cocoa. Product derived from a cocoa probability map where the recommended threshold of >65%, had already been applied.</td><td rowspan="2">Kalischek, N., Lang, N., Renier, C., Daudt, R. C., Addoah, T., Thompson, W., Blaser-Hart, W. J., Garrett, R., Schindler, K., & Wegner, J. D. (2022). <i>Satellite-based high-resolution maps of cocoa planted area for Côte d’Ivoire and Ghana</i> ( 5). arXiv. https://doi.org/10.48550/ARXIV.2206.06119</td></tr>
    <tr></tr>
    <tr><td>ee.Image(‘projects/ee-nk-cocoa/assets/cocoa_map_threshold_065’)</td></tr>
    <tr><td rowspan="3">Cocoa_bnetd</td><td rowspan="3">Commodity class for cocoa (i.e., class 9). For Côte d'Ivoire only.</td><td rowspan="2">BNETD (2024). <i>Occupation des sols de la Côte d'Ivoire en 2020</i> (Version 2).
Centre d'Information Géographique et du Numérique / Bureau National d’Études Techniques et de Developpement. Côte d'Ivoire, 2024. Data available online from: https://arcg.is/0uHOi90</td></tr>
    <tr></tr>
    <tr><td>ee.Image("projects/ee-bnetdcign2/assets/OCS_CI_2020vf")</td></tr>
<tr><td rowspan="3">Rubber_RBGE</td><td rowspan="3">Binary layer for South East Asia.</td><td rowspan="2">Wang et al., (2024) Wang, Y., Hollingsworth, P.M., Zhai, D. et al. High-resolution maps show that rubber causes substantial deforestation. Nature 623, 340–346 (2023). https://doi.org/10.1038/s41586-023-06642-z</td></tr>
    <tr></tr>
    <tr><td>ee.Image("users/wangyxtina/MapRubberPaper/rRubber10m202122_perc1585DifESAdist5pxPF")</td>
    <tr><td rowspan="3">Rubber_FDaP</td><td rowspan="3">Rubber probability model. Filtered collection to 2020 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy.</td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/rubber/model_2024a")</td></tr>
    <tr><td colspan="3"><b>Annual crops:</b></td></tr>
    <tr><td rowspan="3">Soy_Song_2020</td><td rowspan="3">Soya expansion in South America 2000-2023, binary map for 2020 where 1 is soya.</td><td rowspan="2">Song, X.-P., Hansen, M.C., Potapov, P., Adusei, B., Pickering, J., Adami, M., Lima, A., Zalles, V., Stehman, S.V., Di Bella, C.M., Cecilia, C.M., Copati, E.J., Fernandes, L.B., Hernandez-Serna, A., Jantz,  S.M., Pickens, A.H., Turubanova, S., Tyukavina A. (2021). Massive soybean expansion in South America since 2000 and implications for conservation. Nature Sustainability, 4, 784–792
https://doi.org/10.1038/s41893-021-00729-z</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/glad/soy_annual_SA/2020")</td></tr>      
    <tr><td colspan="3"><b>Datasets of disturbances before 2020-12-31:</b></td></tr>
    <tr><td rowspan="3">TMF_deg_before_2020 <br> TMF_def_before_2020</td><td rowspan="3">Binary masks of aggregate degradation & deforestation between 2000 and 2020.</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10). https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection('projects/JRC/TMF/v1_2023/DegradationYear')<br>ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear')</td></tr>
    <tr><td rowspan="3">GFC_loss_before_2020</td><td rowspan="3">Binary mask of aggregate tree cover losses between 2000 and 2020.</td><td rowspan="2">Hansen, M. C., Potapov, P. V., Moore, R., Hancher, M., , S. A., Tyukavina, A., Thau, D., Stehman, S. V., Goetz, S. J., Loveland, T. R., Kommareddy, A., Egorov, A., Chini, L., Justice, C. O., & Townshend, J. R. G. (2013). <i>High-Resolution Global Maps of 21st-Century Forest Cover Change.</i> Science, 342(6160), 850–853. https://doi.org/10.1126/science.1244693. Data available online from: https://glad.earthengine.app/view/global-forest-change.</td></tr>
    <tr></tr>
    <tr><td>ee.Image("UMD/hansen/global_forest_change_2023_v1_11")</td></tr>
    <tr><td rowspan="3">RADD_before_2020</td><td rowspan="3">Binary mask of aggregate confirmed (i.e., class 3) alerts in 2019 & 2020.</td><td rowspan="2">Reiche, J., Mullissa, A., Slagter, B., Gou, Y., Tsendbazar, N.-E., Odongo-Braun, C., Vollrath, A., Weisse, M. J., Stolle, F., Pickens, A., Donchyts, G., Clinton, N., Gorelick, N., & Herold, M. (2021). <i>Forest disturbance alerts for the Congo Basin using Sentinel-1.</i> Environmental Research Letters, 16(2), 024005. https://doi.org/10.1088/1748-9326/abd0a8</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection('projects/radar-wur/raddalert/v1')</td></tr>
    <tr><td rowspan="3">MODIS_fire_before_2020</td><td rowspan="3">Binary mask of aggregate burnt areas between 2000 and 2020.</td><td rowspan="2">Giglio, L., Justice, C., Boschetti, L., & Roy, D. (2021). <i>MODIS/Terra+Aqua Burned Area Monthly L3 Global 500m SIN Grid V061</i> [Dataset]. NASA EOSDIS Land Processes Distributed Active Archive Center. https://doi.org/10.5067/MODIS/MCD64A1.061</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("MODIS/061/MCD64A1")</td></tr>
    <tr><td rowspan="3">ESA_fire_before_2020</td><td rowspan="3">Binary mask of aggregate burnt areas between 2001 and 2020.</td><td rowspan="2">Lizundia-Loiola, J., Otón, G., Ramo, R., & Chuvieco, E. (2020). <i>A spatio-temporal active-fire clustering approach for global burned area mapping at 250 m from MODIS data.</i> Remote Sensing of Environment, 236, 111493. https://doi.org/10.1016/j.rse.2019.111493</a></td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("ESA/CCI/FireCCI/5_1")</td></tr>
  <tr><td colspan="3"><b>Datasets of disturbances after 2020-12-31:</b></td></tr>
    <tr><td rowspan="3">TMF_deg_after_2020 <br> TMF_def_after_2020</td><td rowspan="3">Binary masks of aggregate degradation & deforestation from 2021 onward.</td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). <i>Long-term (1990–2019) monitoring of forest cover changes in the humid tropics.</i> Science Advances, 7(10). https://doi.org/10.1126/sciadv.abe1603</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection('projects/JRC/TMF/v1_2023/DegradationYear')<br>ee.ImageCollection('projects/JRC/TMF/v1_2023/DeforestationYear')</td></tr>
    <tr><td rowspan="3">GFC_loss_after_2020</td><td rowspan="3">Binary mask of aggregate tree cover losses from 2021 onward.</td><td rowspan="2">Hansen, M. C., Potapov, P. V., Moore, R., Hancher, M., , S. A., Tyukavina, A., Thau, D., Stehman, S. V., Goetz, S. J., Loveland, T. R., Kommareddy, A., Egorov, A., Chini, L., Justice, C. O., & Townshend, J. R. G. (2013). <i>High-Resolution Global Maps of 21st-Century Forest Cover Change.</i> Science, 342(6160), 850–853. https://doi.org/10.1126/science.1244693. Data available online from: https://glad.earthengine.app/view/global-forest-change.</td></tr>
    <tr></tr>
    <tr><td>ee.Image("UMD/hansen/global_forest_change_2023_v1_11")</td></tr>
    <tr><td rowspan="3">RADD_after_2020</td><td rowspan="3">Binary mask of aggregate confirmed (i.e., class 3) alerts from 2021 onward.</td><td rowspan="2">Reiche, J., Mullissa, A., Slagter, B., Gou, Y., Tsendbazar, N.-E., Odongo-Braun, C., Vollrath, A., Weisse, M. J., Stolle, F., Pickens, A., Donchyts, G., Clinton, N., Gorelick, N., & Herold, M. (2021). <i>Forest disturbance alerts for the Congo Basin using Sentinel-1.</i> Environmental Research Letters, 16(2), 024005. https://doi.org/10.1088/1748-9326/abd0a8</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection('projects/radar-wur/raddalert/v1')</td></tr>
    <tr><td rowspan="3">MODIS_fire_after_2020</td><td rowspan="3">Binary mask of aggregate burnt areas from 2021 onward.</td><td rowspan="2">Giglio, L., Justice, C., Boschetti, L., & Roy, D. (2021). <i>MODIS/Terra+Aqua Burned Area Monthly L3 Global 500m SIN Grid V061</i> [Dataset]. NASA EOSDIS Land Processes Distributed Active Archive Center. https://doi.org/10.5067/MODIS/MCD64A1.061</td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("MODIS/061/MCD64A1")</td></tr>
    <tr><td colspan="3"><b>Primary forests:</b></td></tr>
    <tr><td rowspan="3">GFT_primary</td><td rowspan="3"> Primary forest class (10) from the Global Forest Types map V0 (forest extent of GFC2020 V1).</td><td rowspan="2">	European Commission: Joint Research Centre, BOURGOIN, C., VERHEGGHEN, A., CARBONI, S., DEGREVE, L., AMEZTOY ARAMENDI, I., CECCHERINI, G., COLDITZ, R. and ACHARD, F., Global Forest Maps for the Year 2020 to Support the EU Regulation on Deforestation-free Supply Chains, Publications Office of the European Union, Luxembourg, 2025, https://data.europa.eu/doi/10.2760/1975879, JRC141702. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“JRC/GFC2020_subtypes/V0”)</td></tr>
    <tr><td rowspan="3">IFL_2020</td><td rowspan="3"> Intact Forest Landscape binary map (1 is IFL).</td><td rowspan="2">Potapov, P., Hansen, M.C., Laestadius, L., Turubanova, S., Yaroshenko, A., Thies, C., Smith, W., Zhuravleva, I., Komarova, A., Minnemeyer, S., others, 2017. The last frontiers of wilderness: Tracking loss of intact forest landscapes from 2000 to 2013. Science Advances 3, e1600821. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“users/potapovpeter/IFL_2020”)</td></tr>
    <tr><td rowspan="3">European_Primary_Forest</td><td rowspan="3"> Harmonized geodatabase of 48 datasets of primary forests spread across 33 European countries.</td><td rowspan="2">	Sabatini, F.M., Bluhm, H., Kun, Z., Aksenov, D., Atauri, J.A., Buchwald, E., Burrascano, S., Cateau, E., Diku, A., Duarte, I.M., Fernández López, Á.B., Garbarino, M., Grigoriadis, N., Horváth, F., Keren, S., Kitenberga, M., Kiš, A., Kraut, A., Ibisch, P.L., Larrieu, L., Lombardi, F., Matovic, B., Melu, R.N., Meyer, P., Midteng, R., Mikac, S., Mikoláš, M., Mozgeris, G., Panayotov, M., Pisek, R., Nunes, L., Ruete, A., Schickhofer, M., Simovski, B., Stillhard, J., Stojanovic, D., Szwagrzyk, J., Tikkanen, O.-P., Toromani, E., Volosyanchuk, R., Vrška, T., Waldherr, M., Yermokhin, M., Zlatanov, T., Zagidullina, A., Kuemmerle, T., 2021. European primary forest database v2.0. Sci Data 8, 220. https://doi.org/10.1038/s41597-021-00988-7 </td></tr>
    <tr></tr>
    <tr><td>ee.FeatureCollection(“HU_BERLIN/EPFD/V2/polygons”)</td></tr>
    <tr><td colspan="3"><b>Naturally regenerating forests:</b></td></tr>
    <tr><td rowspan="3">GFT_naturally_regenerating</td><td rowspan="3"> Naturally regenerating forest class (1) from the Global Forest Types map V0 (forest extent of GFC2020 V1).</td><td rowspan="2">	European Commission: Joint Research Centre, BOURGOIN, C., VERHEGGHEN, A., CARBONI, S., DEGREVE, L., AMEZTOY ARAMENDI, I., CECCHERINI, G., COLDITZ, R. and ACHARD, F., Global Forest Maps for the Year 2020 to Support the EU Regulation on Deforestation-free Supply Chains, Publications Office of the European Union, Luxembourg, 2025, https://data.europa.eu/doi/10.2760/1975879, JRC141702. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“JRC/GFC2020_subtypes/V0”)</td></tr>
    <tr><td colspan="3"><b>Planted/plantation forests:</b></td></tr>
    <tr><td rowspan="3">GFT_planted_plantation</td><td rowspan="3"> Planted and plantation forests class (20) from the Global Forest Types map V0 (forest extent of GFC2020 V1).</td><td rowspan="2">	European Commission: Joint Research Centre, BOURGOIN, C., VERHEGGHEN, A., CARBONI, S., DEGREVE, L., AMEZTOY ARAMENDI, I., CECCHERINI, G., COLDITZ, R. and ACHARD, F., Global Forest Maps for the Year 2020 to Support the EU Regulation on Deforestation-free Supply Chains, Publications Office of the European Union, Luxembourg, 2025, https://data.europa.eu/doi/10.2760/1975879, JRC141702. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“JRC/GFC2020_subtypes/V0”)</td></tr>
    <tr><td rowspan="3">IIASA_planted_plantation</td><td rowspan="3"> Planted or plantation forests classes (31,32) of the IIASA Global Forest Management map.</td><td rowspan="2">	Lesiv, M., Schepaschenko, D., Buchhorn, M., See, L., Dürauer, M., Georgieva, I., Jung, M., Hofhansl, F., Schulze, K., Bilous, A., Blyshchyk, V., Mukhortova, L., Brenes, C.L.M., Krivobokov, L., Ntie, S., Tsogt, K., Pietsch, S.A., Tikhonova, E., Kim, M., Di Fulvio, F., Su, Y.-F., Zadorozhniuk, R., Sirbu, F.S., Panging, K., Bilous, S., Kovalevskii, S.B., Kraxner, F., Rabia, A.H., Vasylyshyn, R., Ahmed, R., Diachuk, P., Kovalevskyi, S.S., Bungnamei, K., Bordoloi, K., Churilov, A., Vasylyshyn, O., Sahariah, D., Tertyshnyi, A.P., Saikia, A., Malek, Ž., Singha, K., Feshchenko, R., Prestele, R., Akhtar, I. ul H., Sharma, K., Domashovets, G., Spawn-Lee, S.A., Blyshchyk, O., Slyva, O., Ilkiv, M., Melnyk, O., Sliusarchuk, V., Karpuk, A., Terentiev, A., Bilous, V., Blyshchyk, K., Bilous, M., Bogovyk, N., Blyshchyk, I., Bartalev, S., Yatskov, M., Smets, B., Visconti, P., Mccallum, I., Obersteiner, M., Fritz, S., 2022. Global forest management data for 2015 at a 100 m resolution. Sci Data 9, 199. https://doi.org/10.1038/s41597-022-01332-3 </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“projects/sat-io/open-datasets/GFM/FML_v3-2”)</td></tr>
    <tr><td colspan="3"><b>Tree cover post 2020:</b></td></tr>
    <tr><td rowspan="3">TMF_regrowth_2023</td><td rowspan="3"> Binary map of Regrowth class (4) for the TMF Annual change year 2023 </td><td rowspan="2">Vancutsem, C., Achard, F., Pekel, J.-F., Vieilledent, G., Carboni, S., Simonetti, D., Gallego, J., Aragão, L. E. O. C., & Nasi, R. (2021). Long-term (1990–2019) monitoring of forest cover changes in the humid tropics. Science Advances, 7(10). https://doi.org/10.1126/sciadv.abe1603 </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“projects/JRC/TMF/v1_2023/AnnualChanges”)</td></tr>
    <tr><td rowspan="3">ESRI_2023_TC</td><td rowspan="3"> Tree cover class (2) of the 2023 ESRI LC map </td><td rowspan="2">Karra, Kontgis, et al. “Global land use/land cover with Sentinel-2 and deep learning.”IGARSS 2021-2021 IEEE International Geoscience and Remote Sensing Symposium. IEEE, 2021. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“"projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS”)</td></tr>
    <tr><td rowspan="3">GLC_FCS30D_TC_2022</td><td rowspan="3"> Forest classes (51 to 92, 181, 185) of the GLC_FCS30D dataset year 2022 </td><td rowspan="2">Liangyun Liu, Xiao Zhang, & Tingting Zhao. (2023). GLC_FCS30D: the first global 30-m land-cover dynamic monitoring product with fine classification system from 1985 to 2022 [Data set]. Zenodo. https://doi.org/10.5281/zenodo.8239305. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“"projects/sat-io/open-datasets/GLC-FCS30D/annual”)</td></tr>
    <tr><td colspan="3"><b>Agricultural land post 2020:</b></td></tr>
    <tr><td rowspan="3">ESRI_2023_crop</td><td rowspan="3"> Crops class (5) of the 2023 ESRI LC map </td><td rowspan="2">Karra, Kontgis, et al. “Global land use/land cover with Sentinel-2 and deep learning.”IGARSS 2021-2021 IEEE International Geoscience and Remote Sensing Symposium. IEEE, 2021. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“"projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS”)</td></tr>
    <tr><td rowspan="3">GLC_FCS30D_crop_2022</td><td rowspan="3"> Crop classes (10 to 20) of the GLC_FCS30D dataset year 2022 </td><td rowspan="2">Liangyun Liu, Xiao Zhang, & Tingting Zhao. (2023). GLC_FCS30D: the first global 30-m land-cover dynamic monitoring product with fine classification system from 1985 to 2022 [Data set]. Zenodo. https://doi.org/10.5281/zenodo.8239305. </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection(“"projects/sat-io/open-datasets/GLC-FCS30D/annual"”)</td></tr>
    <tr><td rowspan="3">Oil_palm_2023_FDaP</td><td rowspan="3"> Palm probability model. Filtered collection to 2023 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy.</td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/palm/model_2024a")</td></tr>
    <tr><td rowspan="3">Rubber_2023_FDaP</td><td rowspan="3"> Cocoa probability model. Filtered collection to 2023 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy. </td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/cocoa/model_2024a")</td></tr>
    <tr><td rowspan="3">Cocoa_2023_FDaP</td><td rowspan="3"> Rubber probability model. Filtered collection to 2023 data. Threshold set for Whisp based on the intersection of recall and precision in charts for accuracy. </td><td rowspan="2">FDaP (2024). Forest Data Partnership https://developers.google.com/earth-engine/datasets/publisher/forestdatapartnership </td></tr>
    <tr></tr>
    <tr><td>ee.ImageCollection("projects/forestdatapartnership/assets/rubber/model_2024a")</td></tr>
    <tr><td colspan="3"><b>logging concessions:</b></td></tr>
    <tr><td rowspan="3">GFW_logging</td><td rowspan="3"> Logging concessions from GFW (polygon data) </td><td rowspan="2">http://data.globalforestwatch.org/datasets?q=logging </td></tr>
    <tr></tr>
    <tr><td>ee.FeatureCollection('projects/ee-whisp/assets/logging/')</td></tr>
    <tr><td colspan="3"><b>Plot location:</b></td></tr>
    <tr><td rowspan="3">Country</td><td rowspan="3">ISO3 code for the country based on plot centroid. Boundaries may contain errors and do not imply official endorsement or acceptance.</td><td rowspan="2">Runfola, D. et al. (2020) geoBoundaries: A global database of political administrative boundaries. <i> PLoS ONE </i> 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866 </td></tr>
    <tr></tr>
    <tr><td>ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")</td></tr>
    <tr><td rowspan="3">Admin_Level_1</td><td rowspan="3">Name of subnational administrative boundary (Level 1), based on plot centroid. Boundaries may contain errors and do not imply official endorsement or acceptance.</td><td rowspan="2">Runfola, D. et al. (2020) geoBoundaries: A global database of political administrative boundaries. <i> PLoS ONE </i> 15(4): e0231866.https://doi.org/10.1371/journal.pone.0231866 </td></tr>
    <tr></tr>
    <tr><td>ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")</td></tr>
    <tr><td rowspan="4">In_watebody</td><td rowspan="4">
 Binary mask for permanent water. Used to detect potential plot location errors based on pixel value for the plot centroid. JRC's Global Surface Water data for inland water bodies (classes 1, 2, or 7 from the transitions layer); areas outside USGS Global Shoreline Vector (GSV) boundaries for marine. </td><td>Pekel, JF., Cottam, A., Gorelick, N., Belward, A.S. (2016) High-resolution mapping of global surface water and its long-term changes. <i> Nature </i> 540, 418-422. (doi:10.1038/nature20584)</td></tr>
    <tr><td>
 ee.Image("JRC/GSW1_4/GlobalSurfaceWater")
 </td></tr>
     <td>Sayre, R., S. Noble, S. Hamann, R. Smith, D. Wright et al., (2019). A new 30 meter resolution global shoreline vector and associated global islands database for the development of standardized ecological coastal units. <i> Journal of Operational Oceanography </i>, 12: sup 2, S47-S56, DOI: 10.1080/1755876X.2018.1529714ee.
    </tr>
    </td>
    <tr>
    <td>
    ee.FeatureCollection('projects/sat-io/open-datasets/shoreline/mainlands'); ee.FeatureCollection('projects/sat-io/open-datasets/shoreline/big_islands'); ee.FeatureCollection('projects/sat-io/open-datasets/shoreline/small_islands');
    </tr>
    </td>
  </tbody>
</table>

<br>
<br>


The sources listed in the table above are analyzed and disaggregated into 171 different layers by the Whisp algorithms, some of which are run directly on Google Earth Engine through *Forest Data Partnership's* account and some of which are in the Python codes of this repository. When a geometry (e.g., a polygon) is scanned with Whisp, the zonal statistics of each of these 171 layers are calculated for that geometry, producing a dataframe that holds the 171 different values for that specific geometry. The 171 layers are listed in [lookup_gee_datasets.csv](src/openforis_whisp/parameters/lookup_gee_datasets.csv). Whisping with [Whisp API](https://whisp.openforis.org/) produces a CSV holding all those values in 171 columns, as well as some metadata and crucially the results of the Whisp EUDR risk analysis, which is explained in the [ReadMe](README.md#whisp_datasets). This risk analysis is based on only a subset of all the layers, some of which are, however, aggregate layers that summarize the results from the other layers. These layers used for the risk analysis are marked for the EUDR risk analysis in [lookup_gee_datasets.csv](src/openforis_whisp/parameters/lookup_gee_datasets.csv) in the right-most column 'use_for_risk' or 'use_for_risk_timber' by value '1'. All layers marked by no value or value '0' do not contribute directly to the EUDR risk analysis, but only indirectly by being part of the aggregate layers.
Overall, the output CSV from Whisping a geometry therefore holds:
- 124 values from disaggregated layers;
- 26 values from aggregate or stand-alone layers crucial to the risk analysis;
- the yes & no answers to the risk categories in the [decision tree](https://github.com/user-attachments/assets/007b5f50-3939-4707-95fa-98be4d56745f) and the final risk category ("low", "high", or "more info needed") for the perennial crops, annual crops, timber and livestock;
- some additional metadata and geographic information, e.g. ID, geometry type, hectares, country, etc...

*(Disclaimer: The number of layers is subject to changes. The number of 171 layers mentioned (124+26) might deviate slightly, but not fundamentally.)*

