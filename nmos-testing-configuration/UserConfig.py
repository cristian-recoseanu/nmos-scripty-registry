from . import Config as CONFIG

CONFIG.ENABLE_HTTPS = False

CONFIG.SDP_PREFERENCES = {
    **CONFIG.SDP_PREFERENCES,
    "channels": 4,
    "exactframerate": "30000/1001"
}

CONFIG.DNS_SD_MODE = 'unicast'
