import pymongo, logging, time
from utils import google_geocode, GoogleError


FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
formatter = logging.Formatter(FORMAT)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('geolocations.log')

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging.basicConfig(format=FORMAT, level=20, handlers=[c_handler, f_handler])
logger = logging.getLogger('geolocations')


logger.info("Starting Get Location Service.")
while True:
    try:
        myclient = pymongo.MongoClient("mongodb://192.168.1.61:27017/")
        with myclient:
            logger.info("Connected to DB.")
            frm = "{0:>30} {1:7}"

            for a in list(myclient.ss_ads.ads.distinct("address_lv", {})):
                if not a or a.endswith('..'):
                    continue
                address_geodata = list(myclient.ss_ads.geodata.find({'address': a}))
                if not address_geodata:
                    logger.info("Processing: %s", a)
                    done = False
                    while not done:
                        try:
                            geocode_result = google_geocode(a, key='AIzaSyCasbDiMWMftbKcSnFrez-SF-YCechHSLA')
                            myclient.ss_ads.geodata.insert({'address': a, 'geodata':geocode_result})
                            logger.info(list(myclient.ss_ads.geodata.find({'address': a})))
                            done = True
                        except GoogleError as e:
                            logger.error("%s %s", a, e)
                            time.sleep(0.2)

            logger.info("Waiting: %s s.", 60)
            time.sleep(60)

    except RuntimeError as e:
        logger.error(e)
