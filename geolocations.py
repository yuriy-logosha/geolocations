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


def get_addresses_to_process(db):
    geo_empty = list(db.geodata.distinct('address', {"geodata": []}) )
    geo_empty.sort(reverse=True)

    geo_address = list(db.geodata.distinct('address', {}) )
    total_address = list(db.ads.distinct("address_lv", kind_ad))
    result = list(set(list(set(total_address) - set(geo_address)) + geo_empty))
    result.sort()
    return result


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
                        geocode_result = google_geocode(a, key='AIzaSyCasbDiMWMftbKcSnFrez-SF-YCechHSLA')
                        exist = list(myclient.ss_ads.geodata.find({'address': a}))
                        if len(exist) > 0:
                            if geocode_result:
                                myclient.ss_ads.geodata.update_one({'_id': exist[0]['_id']}, {'$set': {'geodata': geocode_result}})
                        else:
                            myclient.ss_ads.geodata.insert_one({'address': a, 'geodata': geocode_result})
                        logger.info(list(myclient.ss_ads.geodata.find({'address': a})))
                        done = True
                    except GoogleError as e:
                        # logger.info("Processing: %s %s/%s %s", a, diff.index(a), len(diff), e.status)
                        time.sleep(0.1)
                    except Exception as e:
                        logger.error(e)

            logger.info("Waiting: %s s.", 60)
            time.sleep(60)

    except Exception as e:
        logger.error(e)
