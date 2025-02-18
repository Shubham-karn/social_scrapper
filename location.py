import pandas as pd
import os

cities = [
        "outram-singapore",
        "singapore-singapore",
        "rochor-singapore",
        "rivervalley-singapore",
        "city-singapore",
        "kallang-singapore",
        "geylang-singapore",
        "dhobyghaut-singapore",
        "thomson-singapore",
        "singaporeriver-singapore",
        "yishun-singapore",
        "kampongwaktanjong-singapore",
        "woodlandsnewtown-singapore",
        "tampinesnewtown-singapore",
        "kampongjagoh-singapore",
        "toapayoh-singapore",
        "kampongkebunbaharu-singapore",
        "playfairestate-singapore",
        "marineparade-singapore",
        "brickworksestate-singapore",
        "huatchoevillage-singapore",
        "tampinesestate-singapore",
        "bukitmerahestate-singapore",
        "kampongtebingterjun-singapore",
        "hougang-singapore",
        "boonlayresettlementarea-singapore",
        "mokpenghiangestate-singapore",
        "sembawang-singapore",
        "matildaestate-singapore",
        "toapayohnewtown-singapore",
        "marina-singapore",
        "centralregion-singapore",
        "seaviewestate-singapore",
        "kampongtongkangpechah-singapore",
        "bukitpanjangnewtown-singapore",
        "angmokiovillage-singapore",
        "ulubedokvillage-singapore",
        "happygardens-singapore",
        "sennettestate-singapore",
        "bishan-singapore",
        "kampongtampines-singapore",
        "kampongayergemuruh-singapore",
        "kalang-singapore",
        "jurongeast-singapore",
        "serangoon-singapore",
        "north-eastregion-singapore",
        "chaicheeestate-singapore",
        "kampongloyang-singapore",
        "kampongdaratnanas-singapore",
        "tuas-singapore",
        "bukitmerah-singapore",
        "kampongubi-singapore",
        "jurongvillage-singapore",
        "angmokio-singapore",
        "yewtee-singapore",
        "kakibukitestate-singapore",
        "serangoongardenestate-singapore",
        "kampongserangoonkechil-singapore",
        "hougangnewtown-singapore",
        "mayflowerestate-singapore",
        "jurongwest-singapore",
        "simpangbedok-singapore",
        "kampongsireh-singapore",
        "ongleevillage-singapore",
        "swisscottageestate-singapore",
        "bukitpanjang-singapore",
        "bukittimahvillage-singapore",
        "chiakeng-singapore",
        "kampongulujurong-singapore",
        "bukitsembawangestate-singapore",
        "bedok-singapore",
        "thomsonpark-singapore",
        "kampongpengkalankundor-singapore",
        "yishunnewtown-singapore",
        "tampines-singapore",
        "bukitpanjangestate-singapore",
        "kampongamoyquee-singapore",
        "kampongsungipinang-singapore",
        "kampongpasirris-singapore",
        "keathong-singapore",
        "simei-singapore",
        "jurongwestnewtown-singapore",
        "kampongbatak-singapore",
        "kentridge-singapore",
        "bukitbatok-singapore",
        "kampongulupandan-singapore",
        "eastregion-singapore",
        "kampongradinmas-singapore",
        "tanglinhalt-singapore",
        "geylangseraivillage-singapore",
        "northregion-singapore",
        "bishannewtown-singapore",
        "marsilingestate-singapore",
        "choachukangnewtown-singapore",
        "taykengloonestate-singapore",
        "woodlands-singapore",
        "heapguan-singapore",
        "jurongeastnewtown-singapore",
        "taiseng-singapore",
        "pasirris-singapore",
        "frankelestate-singapore",
        "pulaubrani-singapore",
        "kembangan-singapore",
        "westcoastvillage-singapore",
        "princeedwardpoint-singapore",
        "kampongbukitpanjang-singapore",
        "kampongsungaitengah-singapore",
        "westregion-singapore",
        "yewteevillage-singapore",
        "teachershousingestate-singapore",
        "marsiling-singapore",
        "rosegarden-singapore",
        "chinesegardens-singapore",
        "sengkang-singapore",
        "sussexestate-singapore",
        "sitestate-singapore",
        "kampongreteh-singapore",
        "changivillage-singapore",
        "mountvernon-singapore",
        "kampongseklim-singapore",
        "buonavista-singapore",
        "operaestate-singapore",
        "simpang-singapore",
        "serangoonnewtown-singapore",
        "hollandvillage-singapore",
        "hongkah-singapore",
        "kampongcutforth-singapore",
        "engkhonggardens-singapore",
        "clementi-singapore",
        "easterngardens-singapore",
        "kamwakhassan-singapore",
        "bamboogrovepark-singapore",
        "sarangrimau-singapore",
        "kampongeunos-singapore",
        "boonlay-singapore",
        "thomsongardenestate-singapore",
        "lokyang-singapore",
        "thomsonridgeestate-singapore",
        "kampongsanteng-singapore",
        "princesselizabethestate-singapore",
        "rochesterpark-singapore",
        "chyekay-singapore",
        "kamponglewlian-singapore",
        "fuyongestate-singapore",
        "thomsonriseestate-singapore",
        "queenstown-singapore",
        "chongpang-singapore",
        "kampongmandaikechil-singapore",
        "kingalbertpark-singapore",
        "nepalpark-singapore",
        "kampongpachitan-singapore",
        "pasirrisvillage-singapore",
        "kampongsungaiblukar-singapore",
        "kampongchangi-singapore",
        "phoenixpark-singapore",
        "tanglin-singapore",
        "boonlay-singapore",
        "sagavillage-singapore",
        "changi-singapore",
        "clementinewtown-singapore",
        "marineparadeestate-singapore",
        "sembawanghillsestate-singapore",
        "pandanvalley-singapore",
        "faberhills-singapore",
        "serangoonvillage-singapore",
        "sembawangvillage-singapore",
        "edenpark-singapore",
        "kampongtanjongpenjuru-singapore",
        "queenstownnewtown-singapore",
        "caldecotthillestate-singapore",
        "kampongsultan-singapore",
        "medwaypark-singapore",
        "jalankayu-singapore",
        "mataikanvillage-singapore",
        "hongleonggarden-singapore",
        "fabergarden-singapore",
        "ayerrajah-singapore",
        "hunyeangvillage-singapore",
        "kangkar-singapore",
        "ghimmoh-singapore",
        "wattenestate-singapore",
        "pasirpanjang-singapore",
        "neesoonvillage-singapore",
        "punggolvillage-singapore",
        "mandai-singapore",
        "braddellheightsestate-singapore",
        "bukitmandai-singapore",
        "sungeikadut-singapore",
        "kampongkembangan-singapore",
        "wessexestate-singapore",
        "novena-singapore",
        "humeheights-singapore",
        "bukitbatoknewtown-singapore",
        "yiochukangestate-singapore",
        "tanglinhill-singapore",
        "siewlimpark-singapore",
        "eastviewgarden-singapore",
        "sommervilleestate-singapore",
        "kampongwoodleigh-singapore",
        "kampongpesek-singapore",
        "springparkestate-singapore",
        "xilinestate-singapore",
        "chiphockgarden-singapore",
        "pasirpanjangvillage-singapore",
        "choachukang-singapore",
        "dunearnestate-singapore",
        "yiochukang-singapore",
        "kilburnestate-singapore",
        "hongkongpark-singapore",
        "kampongharvey-singapore",
        "princeedwardpark-singapore",
        "somapahchangi-singapore",
        "sungaimandai-singapore",
        "citywest-singapore",
        "kingsmeadhall-singapore",
        "kampongocarrollscott-singapore",
        "kampongpunggol-singapore",
        "kampongtanahmerahkechil-singapore",
        "hollandgrovepark-singapore",
        "sungaisimpang-singapore",
        "kampongblukang-singapore",
        "tuakanglye-singapore",
        "bukittimah-singapore",
        "seletar-singapore",
        "kampongkranji-singapore",
        "kampongwakhassan-singapore",
        "kampongkitin-singapore",
        "rafflespark-singapore",
        "springleafpark-singapore",
        "punggol-singapore",
        "kampongbelimbing-singapore",
        "kampongsungaijurong-singapore",
        "kampongjavateban-singapore",
        "hillcrestpark-singapore",
        "racecoursevillage-singapore",
        "kampongchantekbaharu-singapore",
        "kampongwingloong-singapore",
        "kranji-singapore",
        "bedok-singapore",
        "padangterbakarvillage-singapore",
        "lamsan-singapore",
        "coronationgardens-singapore",
        "camdenpark-singapore",
        "jurongisland-singapore",
        "islandviewestate-singapore",
        "shamrockpark-singapore",
        "windsorparkestate-singapore",
        "simeiestate-singapore",
        "simeinewtown-singapore",
        "luckyhills-singapore",
        "westerncatchmentarea-singapore",
        "kampongamber-singapore",
        "woodleighpark-singapore",
        "brizaypark-singapore",
        "mcmahonpark-singapore",
        "peoplesgarden-singapore",
        "tiongguanestate-singapore",
        "bulimvillage-singapore",
        "yiochukang-singapore",
        "wattenpark-singapore",
        "namazieestate-singapore",
        "mountpleasant-singapore",
        "teckchongestate-singapore",
        "bedokville-singapore",
        "marylandestate-singapore",
        "limchukang-singapore",
        "kampongsiren-singapore",
        "centralcatchmentarea-singapore",
        "ngkayboonestate-singapore",
        "yankitvillage-singapore",
        "bajauestate-singapore",
        "aikhongandaikchiangestate-singapore",
        "charltonpark-singapore",
        "kampongpulaukechil-singapore",
        "kampongwakbechik-singapore",
        "kampongtanahmerah-singapore",
        "ewartpark-singapore",
        "tianguanestate-singapore",
        "kianhongestate-singapore",
        "kampongsudong-singapore",
        "luckypark-singapore",
        "kampongayermerbau-singapore",
        "brighthillcrescent-singapore",
        "pulautekong-singapore",
        "bintongpark-singapore",
        "southern-singapore",
        "songhahestate-singapore",
        "kampongberemban-singapore",
        "queenastridpark-singapore",
        "thonghoe-singapore",
        "taikhenggardens-singapore",
        "kampongwaksekak-singapore",
        "rebeccapark-singapore",
        "banguanpark-singapore",
        "limchukangestate-singapore",
        "kampongjelutong-singapore",
        "haisingpark-singapore",
        "kampongpengkalanpetai-singapore",
        "bedokgarden-singapore",
        "kampongsakeng-singapore",
        "benapark-singapore",
        "kampongbugis-singapore",
        "kampongbatukoyok-singapore",
        "kampongperigipiau-singapore",
        "kimchuan-singapore",
        "leedonpark-singapore",
        "kampongpulauubin-singapore",
        "kampongtengah-singapore",
        "pulaubukom-singapore",
        "kampongbereh-singapore",
        "kampongteban-singapore",
        "kampongwahtuang-singapore",
        "hongkahvillage-singapore",
        "pulauubin-singapore",
        "sinwattestate-singapore",
        "kampongnoordin-singapore",
        "payalebar-singapore",
        "oeitionghampark-singapore",
        "victoriapark-singapore",
        "tengah-singapore",
        "kampongchejevah-singapore",
        "punggolestate-singapore",
        "kampongsanyonkongparit-singapore",
        "amakengvillage-singapore",
        "teckhockvillage-singapore",
        "ahsoogarden-singapore",
        "kampongtelok-singapore",
        "kampongmalayu-singapore",
        "tanhuagekestate-singapore",
        "lamkiongestate-singapore",
        "kampongpahang-singapore",
        "seminoi-singapore",
        "samhinestate-singapore",
        "kampongbahru-singapore",
        "kampongayersamak-singapore",
        "chiatongquahestate-singapore",
        "kampongsanyongkong-singapore",
        "kampongayerbajau-singapore",
        "kampongmamam-singapore",
        "kampongdulah-singapore",
        "kampongtodak-singapore",
        "kampongunum-singapore",
        "fishfarmingestate-singapore",
        "kampongpengkalanpakau-singapore",
        "kiantecksanestate-singapore",
        "kampongpermatang-singapore",
        "kampongpasir-singapore",
        "kampongsungaibelang-singapore",
        "kampongpasirmerah-singapore",
        "sungaiunumestate-singapore",
        "kampongsalabin-singapore",
        "kampongayersamakdarat-singapore"
    ]

async def get_city():
    try:
        return {
            "status_code": 200,
            "data": cities
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }

async def get_city_url(city):
    try:
        data = pd.read_csv('city_data.csv')
        if city not in cities:
            return {
                "status_code": 404,
                "error": "City not found"
            }
        directory = 'location'
        for filename in os.listdir(directory):
            if filename.startswith(city) and filename.endswith('.csv'):
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path)
                return {
                    "status_code": 200,
                    "data": {
                        "url": data[data['city_name'] == city]['url'].values[0],
                        "location": df['cityname'].tolist()
                    
                    }
                }
        return {
            "status_code": 200,
            "data": data[data['city_name'] == city]['url'].values[0]
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }

async def get_location(city, place):
    try:
        if city not in cities:
            return {
                "status_code": 404,
                "error": "City not found"
            }
        directory = 'location'
        for filename in os.listdir(directory):
            if filename.startswith(city) and filename.endswith('.csv'):
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path)
                return {
                    "status_code": 200,
                    "data": {
                        "location": place,
                        "url": df[df['cityname'] == place]['url'].values[0]
                    }
                }
        return {
            "status_code": 404,
            "error": "Place not found in {}".format(city)
        }
    except Exception as e:
        if str(e) == "index 0 is out of bounds for axis 0 with size 0":
            return {
                "status_code": 404,
                "error": "No place with name {} found in {}".format(place, city)
            }
        else:
            return {
                "status_code": 500,
                "error": str(e)
            }

