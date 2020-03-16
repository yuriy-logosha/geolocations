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
kind_ad = {'kind':'ad'}


def get_diff(db):
    geo_address = list(db.geodata.distinct('address', {}))
    total_address = list(db.ads.distinct("address_lv", kind_ad))
    return list(set(total_address) - set(geo_address))


logger.info("Starting Get Location Service.")
while True:
    try:
        myclient = pymongo.MongoClient("mongodb://192.168.1.61:27017/")
        with myclient:
            logger.info("Connected to DB.")
            frm = "{0:>30} {1:7}"

            diff = get_diff(myclient.ss_ads)

            for a in diff:
                if not a or a.endswith('..'):
                    continue
                logger.info("Processing: %s %s/%s", a, diff.index(a), len(diff))
                done = False
                while not done:
                    try:
                        geocode_result = google_geocode(a, key='AIzaSyCasbDiMWMftbKcSnFrez-SF-YCechHSLA')
                        myclient.ss_ads.geodata.insert({'address': a, 'geodata': geocode_result})
                        logger.info(list(myclient.ss_ads.geodata.find({'address': a})))
                        done = True
                    except GoogleError as e:
                        logger.info("Processing: %s %s/%s %s", a, diff.index(a), len(diff), e)
                        time.sleep(0.1)

            logger.info("Waiting: %s s.", 60)
            time.sleep(60)

    except RuntimeError as e:
        logger.error(e)
