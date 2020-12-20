import logging
import pymongo
import time

from utils import google_geocode, GoogleError
from logging.handlers import RotatingFileHandler

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
formatter = logging.Formatter(FORMAT)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.handlers.RotatingFileHandler('geolocations.log', mode='a', maxBytes=5*1024*1024, backupCount=10, encoding=None, delay=0)

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging.basicConfig(format=FORMAT, level=20, handlers=[c_handler, f_handler])
logger = logging.getLogger('geolocations')
kind_ad = {'kind':'ad'}


def get_addresses_to_process(db):
    geo_address = list(db.geodata.distinct('address', {}) )
    total_address = list(db.ads.distinct("address_lv", kind_ad))
    missed = list(set(total_address) - set(geo_address))
    missed.sort()

    return missed


logger.info("Starting Get Location Service.")
while True:
    try:
        myclient = pymongo.MongoClient("mongodb://192.168.1.61:27017/")
        with myclient:
            logger.info("Connected to DB.")
            frm = "{0:>30} {1:7}"

            addresses_to_process = get_addresses_to_process(myclient.ss_ads)

            for a in addresses_to_process:
                if not a or a.endswith('..'):
                    logger.info("Skip: %s %s/%s", a, addresses_to_process.index(a), len(addresses_to_process))
                    continue
                logger.info("Processing: %s %s/%s", a, addresses_to_process.index(a), len(addresses_to_process))
                done = False
                while not done:
                    try:
                        geocode_result = google_geocode('riga '+a, key='AIzaSyCasbDiMWMftbKcSnFrez-SF-YCechHSLA')
                        if not geocode_result:
                            geocode_result = google_geocode(a, key='AIzaSyCasbDiMWMftbKcSnFrez-SF-YCechHSLA')

                        exist = list(myclient.ss_ads.geodata.find({'address': a}))
                        if len(exist) > 0:
                            myclient.ss_ads.geodata.update_one({'_id': exist[0]['_id']}, {'$set': {'geodata': geocode_result}})
                        else:
                            myclient.ss_ads.geodata.insert_one({'address': a, 'geodata': geocode_result})
                        logger.info(list(myclient.ss_ads.geodata.find({'address': a})))
                        done = True
                    except GoogleError as e:
                        # logger.info("Processing: %s %s/%s %s", a, addresses_to_process.index(a), len(addresses_to_process), e.status)
                        time.sleep(0.1)
                    except Exception as e:
                        logger.error(e)

            logger.info("Waiting: %s s.", 60)
            time.sleep(60)
            addresses_to_process = get_addresses_to_process(myclient.ss_ads)

    except Exception as e:
        logger.error(e)
        time.sleep(5)

logger.info("Stopping Get Location Service.")